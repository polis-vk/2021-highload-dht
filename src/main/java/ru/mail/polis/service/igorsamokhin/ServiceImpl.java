package ru.mail.polis.service.igorsamokhin;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import one.nio.util.ByteArrayBuilder;
import one.nio.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

public class ServiceImpl extends HttpServer implements Service {
    private static final String ENDPOINT_V0_STATUS = "/v0/status";
    private static final String ENDPOINT_V0_ENTITY = "/v0/entity";
    private static final String ENDPOINT_V0_RANGE = "/v0/entities";
    private final Logger logger = LoggerFactory.getLogger(ServiceImpl.class);

    public static final String BAD_ID_RESPONSE = "Bad id";
    public static final int MAXIMUM_POOL_SIZE = 4;

    private final DAO dao;
    private boolean isWorking; //false by default

    private final ForkJoinPool executor = new ForkJoinPool(MAXIMUM_POOL_SIZE);

    private final ProxyClient proxyClients;

    public ServiceImpl(int port, DAO dao, Set<String> topology) throws URISyntaxException, IOException {
        super(from(port));
        this.dao = dao;
        this.proxyClients = new ProxyClient(MAXIMUM_POOL_SIZE, topology, port);
    }

    private static HttpServerConfig from(int port) {
        HttpServerConfig config = new HttpServerConfig();
        AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        acceptor.reusePort = true;

        config.acceptors = new AcceptorConfig[]{acceptor};
        config.selectors = 1;

        return config;
    }

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {
        if (!isWorking) {
            sendResponse(session, Utils.serviceUnavailableResponse());
            return;
        }

        super.handleRequest(request, session);
    }

    @Override
    public HttpSession createSession(Socket socket) {
        return new Session(socket, this);
    }

    private void sendResponse(HttpSession session, @Nullable Response response) {
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            logger.info("Can't send response ", e);
        }
    }

    @Override
    public synchronized void start() {
        super.start();
        isWorking = true;
    }

    @Override
    public synchronized void stop() {
        isWorking = false;
        executor.shutdown();

        try {
            proxyClients.shutdown();
            if (!executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                throw new IllegalStateException("Can't await termination on close");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        super.stop();
    }

    @Path(ENDPOINT_V0_RANGE)
    public void range(HttpSession session,
                      Request request,
                      @Param(value = "start", required = true) String start,
                      @Param("end") String end) {
        executor.execute(() -> {
            ByteBuffer startKey = ByteBuffer.wrap(start.getBytes(StandardCharsets.UTF_8));
            if (start.isBlank()) {
                sendResponse(session, Utils.badRequest(BAD_ID_RESPONSE));
                return;
            }

            ByteBuffer endKey = null;
            if (end != null) {
                if (end.isBlank()) {
                    sendResponse(session, Utils.badRequest(BAD_ID_RESPONSE));
                    return;
                }
                endKey = ByteBuffer.wrap(end.getBytes(StandardCharsets.UTF_8));
            }

            Iterator<Record> range = dao.range(startKey, endKey);
            try {
                Response ok = new Response(Response.OK);
                ok.addHeader("Transfer-Encoding: chunked");
                ((Session) session).sendChunkedResponse(ok, () -> {
                    if (!range.hasNext()) {
                        return null;
                    }
                    Record next = range.next();
                    ByteArrayBuilder builder = new ByteArrayBuilder(next.size());
                    builder.append(extractBytes(next.getKey()))
                            .append("\n")
                            .append(extractBytes(next.getValue()));
                    return builder.trim();
                });
            } catch (IOException e) {
                logger.error("Can't send range response", e);
            }
        });
    }

    @Path(ENDPOINT_V0_STATUS)
    public Response status() {
        return Response.ok("I'm OK");
    }

    @Path(ENDPOINT_V0_ENTITY)
    public void entity(HttpSession session,
                       Request request,
                       @Param(value = "id", required = true) String id,
                       @Param("replicas") String replicas) {
        executor.execute(() -> {
            if (replicas == null) {
                task(session, request, id, quorum(proxyClients.size()), proxyClients.size());
                return;
            }

            int ack;
            int from;
            try {
                StringTokenizer tokenizer = new StringTokenizer(replicas, "/");
                if (tokenizer.countTokens() != 2) {
                    throw new Exception("Wrong params");
                }

                ack = Integer.parseInt(tokenizer.nextToken());
                from = Integer.parseInt(tokenizer.nextToken());

                if ((ack > from) || (ack <= 0)) {
                    throw new Exception("Wrong params");
                }
            } catch (Exception e) {
                sendResponse(session, Utils.badRequest());
                return;
            }
            task(session, request, id, ack, from);
        });
    }

    private int quorum(int n) {
        return n / 2 + 1;
    }

    private void task(HttpSession session, Request request, String id, int ack, int from) {
        Response response;
        try {
            List<Integer> nodeIds = proxyClients.getNodeId(id);
            if (nodeIds.isEmpty()) {
                response = Utils.badRequest(BAD_ID_RESPONSE);
            } else if (request.getHeader(Utils.PROXY_HEADER_ONE_NIO) != null) {
                response = handleEntity(request, id, true);
            } else if ((nodeIds.size() == 1) && proxyClients.isMe(nodeIds.get(0))) {
                response = handleEntity(request, id, false);
            } else {
                Callable<Response> thisNodeHandler = () -> handleEntity(request, id, true);
                proxyClients.proxy(session, request, nodeIds, ack, from, thisNodeHandler);
                return;
            }
        } catch (RuntimeException e) {
            logger.error("Something wrong in task. ack: {}, from: {}", ack, from, e);
            response = Utils.serviceUnavailableResponse();
        } catch (Exception e) {
            logger.error("Exception in entity handling. Request:\n{} ", request, e);
            response = Utils.serviceUnavailableResponse();
        }
        sendResponse(session, response);
    }

    @Override
    public void handleDefault(
            final Request request,
            final HttpSession session) throws IOException {
        session.sendResponse(Utils.badRequest());
    }

    private Response get(@Param(value = "id", required = true) String id, boolean proxyRequest) {
        ByteBuffer fromKey = wrapString(id);
        ByteBuffer toKey = DAO.nextKey(fromKey);

        Iterator<Record> range = dao.range(fromKey, toKey, proxyRequest);
        if (!range.hasNext()) {
            return Utils.notFoundResponse();
        }

        Record record = range.next();
        byte[] responseBody;

        if (proxyRequest) {
            long timeStamp = record.getTimeStamp();
            int valueSize = record.getValueSize();

            ByteBuffer body = ByteBuffer.allocate(valueSize + Long.BYTES);
            if (valueSize != 0) {
                body.put(record.getValue());
            }
            body.putLong(timeStamp);
            responseBody = body.array();
            Response ok = Response.ok(responseBody);

            if (record.isTombstone()) {
                ok.addHeader(Utils.TOMBSTONE_HEADER_ONE_NIO);
            }
            return ok;
        } else {
            responseBody = extractBytes(record.getValue());
            return Response.ok(responseBody);
        }
    }

    private Response put(@Param(value = "id", required = true) String id, Request request) {
        Record record = Record.of(wrapString(id), ByteBuffer.wrap(request.getBody()), timeStamp());
        dao.upsert(record);
        return Utils.createdResponse();
    }

    private long timeStamp() {
        return System.currentTimeMillis();
    }

    private Response delete(@Param(value = "id", required = true) String id) {
        ByteBuffer key = wrapString(id);
        dao.upsert(Record.tombstone(key, timeStamp()));

        return Utils.acceptedResponse();
    }

    private ByteBuffer wrapString(String string) {
        return ByteBuffer.wrap(Utf8.toBytes(string));
    }

    private static byte[] extractBytes(final ByteBuffer buffer) {
        final byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    private Response handleEntity(Request request, String id, boolean includeTombstones) {
        Response response;
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                response = get(id, includeTombstones);
                break;
            case Request.METHOD_PUT:
                response = put(id, request);
                break;
            case Request.METHOD_DELETE:
                response = delete(id);
                break;
            default:
                response = Utils.badRequest();
                break;
        }
        return response;
    }
}
