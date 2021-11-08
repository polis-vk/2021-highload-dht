package ru.mail.polis.service.igorsamokhin;

import one.nio.http.HttpClient;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;
import one.nio.server.AcceptorConfig;
import one.nio.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ServiceImpl extends HttpServer implements Service {
    private static final String ENDPOINT_V0_STATUS = "/v0/status";
    private static final String ENDPOINT_V0_ENTITY = "/v0/entity";
    public static final String PROXY_HEADER = "Proxy";
    public static final String TOMBSTONE_HEADER = "Tombstone";
    private final Logger logger = LoggerFactory.getLogger(ServiceImpl.class);

    public static final String BAD_ID_RESPONSE = "Bad id";
    public static final int MAXIMUM_POOL_SIZE = 16;
    public static final int KEEP_ALIVE_TIME = 10;

    private final DAO dao;
    private String[] topology;
    private HttpClient[] clients;
    private int id;
    private boolean isWorking; //false by default

    private final ThreadPoolExecutor executor =
            new ThreadPoolExecutor(MAXIMUM_POOL_SIZE,
                    MAXIMUM_POOL_SIZE,
                    KEEP_ALIVE_TIME,
                    TimeUnit.MINUTES,
                    new LinkedBlockingQueue<>());

    private final ThreadPoolExecutor proxyExecutor;

    public ServiceImpl(int port, DAO dao, Set<String> topology) throws IOException {
        this(port, dao, topology, 3);
    }

    public ServiceImpl(int port, DAO dao, Set<String> topology, int replicationFactor) throws IOException {
        super(from(port));
        this.dao = dao;
        this.topology = new String[topology.size()];
        this.clients = new HttpClient[topology.size()];
        this.proxyExecutor = new ThreadPoolExecutor(
                MAXIMUM_POOL_SIZE,
                MAXIMUM_POOL_SIZE * replicationFactor,
                KEEP_ALIVE_TIME,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>());

        String[] t = topology.toArray(new String[0]);
        Arrays.sort(t);
        for (int i = 0; i < t.length; i++) {
            String adr = t[i];
            ConnectionString conn = new ConnectionString(adr + "?timeout=100");
            this.topology[i] = adr;
            if (conn.getPort() == port) {
                this.id = i;
                this.clients[i] = new HttpClient(new ConnectionString(""));//fixme
                continue;
            }

            this.clients[i] = new HttpClient(conn);
        }
    }

    private static HttpServerConfig from(int port) {
        HttpServerConfig config = new HttpServerConfig();
        AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        acceptor.reusePort = true;

        config.acceptors = new AcceptorConfig[]{acceptor};

        return config;
    }

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {
        if (!isWorking) {
            sendResponse(session, UtilResponses.serviceUnavailableResponse());
            return;
        }
        super.handleRequest(request, session);
    }

    private void sendResponse(HttpSession session, @Nullable Response response) {
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            logger.info("Can't send response ", e);
        }
    }

    private List<Integer> getNodeId(String id) {
        if (id.isBlank()) {
            return new ArrayList<>();
        }

        PriorityQueue<Pair<Integer, Integer>> topologyIds = new PriorityQueue<>(Comparator.comparing(a -> a.first));
        for (int i = 0; i < topology.length; i++) {
            int hashCode = (topology[i] + id).hashCode();
            topologyIds.add(new Pair<>(hashCode, i));
        }

        ArrayList<Integer> list = new ArrayList<>(topologyIds.size());
        while (topologyIds.peek() != null) {
            list.add(topologyIds.poll().second);
        }

        return list;
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
        proxyExecutor.shutdown();

        try {
            boolean wasException = !executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.NANOSECONDS);

            if (!proxyExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                wasException = true;
            }

            if (wasException) {
                throw new IllegalStateException("Can't await termination on close");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (clients != null) {
            for (HttpClient client : clients) {
                client.clear();
            }
        }
        clients = null;
        topology = null;
        super.stop();
    }

    @Path(ENDPOINT_V0_STATUS)
    public Response status() {
        return Response.ok("I'm OK");
    }

    @Path(ENDPOINT_V0_ENTITY)
    public void entity(HttpSession session,
                       Request request,
                       @Param(value = "id", required = true) String id,
                       @Param(value = "replicas", required = false) String replicas) {
        executor.execute(() -> {
            if (replicas == null) {
                task(session, request, id, 1, 1);
                return;
            }

            StringTokenizer tokenizer = new StringTokenizer(replicas, "/");
            assert tokenizer.countTokens() == 2;
            int ack;
            int from;
            try {
                ack = Integer.parseInt(tokenizer.nextToken());
                from = Integer.parseInt(tokenizer.nextToken());
            } catch (Exception e) {
                sendResponse(session, UtilResponses.badRequest());
                logger.error("Not valid replicas param: {}\n request: {}", replicas, request);
                return;
            }
            task(session, request, id, ack, from);
        });
    }

    private void task(HttpSession session, Request request, String id, int ack, int from) {
        Response response;
        try {
            List<Integer> nodeIds = getNodeId(id);
            if (nodeIds.isEmpty()) {
                response = UtilResponses.badRequest(BAD_ID_RESPONSE);
            } else if (request.getHeader(PROXY_HEADER) != null) {
                response = handleEntity(request, id, true);
            } else if ((nodeIds.size() == 1) && (nodeIds.get(0) == this.id)) {
                response = handleEntity(request, id, false);
            } else {
                response = proxy(request, id, nodeIds, ack, from);
            }
        } catch (RuntimeException e) {
            logger.error("Something wrong in task. ack: {}, from: {}", ack, from, e);
            response = UtilResponses.serviceUnavailableResponse();
        } catch (PoolException e) {
            logger.error("Perhaps one of nodes in cluster is not responding\n Request:\n{} ", request, e);
            response = UtilResponses.serviceUnavailableResponse();
        } catch (Exception e) {
            logger.error("Exception in entity handling. Request:\n{} ", request, e);
            response = UtilResponses.serviceUnavailableResponse();
        }
        sendResponse(session, response);
    }

    private Response proxy(Request request, String id, List<Integer> ids, int ack, int from) throws PoolException {
        request.addHeader(PROXY_HEADER);

        List<Response> responses = new ArrayList<>(from);
        int confirms = 0;
        int i;
        for (i = 0; i < from; i++) {
            Integer httpClientId = ids.get(i);
            Response response = askHttpClient(request, id, httpClientId);
            responses.add(response);
            confirms += isConfirm(response) ? 1 : 0;
            if (confirms >= ack) {
                break;
            }
        }

        if (confirms < ack) {
            return UtilResponses.responseWithMessage("504", "Not Enough Replicas");
        }

        final Response result;
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                result = mergeGetResponses(responses);
                break;
            case Request.METHOD_PUT:
                result = UtilResponses.createdResponse();
                break;
            case Request.METHOD_DELETE:
                result = UtilResponses.acceptedResponse();
                break;
            default:
                result = UtilResponses.badRequest();
                break;
        }

        int finalI = i + 1;
        if (finalI < from) {
            executor.execute(() -> {
                for (int j = finalI; j < from; j++) {
                    Integer httpClientId = ids.get(j);
                    askHttpClient(request, id, httpClientId);
                }
                //read repair
            });
        }

        return result;
    }

    private Response mergeGetResponses(List<Response> responses) {
        Response tmp = null;
        long timeStamp = -2;

        for (Response response : responses) {
            long l = parseTimeStamp(response);
            if (l > timeStamp) {
                timeStamp = l;
                tmp = response;
            }
        }
        if (timeStamp < 0) {
            return UtilResponses.notFoundResponse();
        }

        Response result;
        try {
            if (tmp.getHeader(TOMBSTONE_HEADER) != null) {
                result = UtilResponses.notFoundResponse();
            } else {
                byte[] body = tmp.getBody();
                byte[] newBody = new byte[body.length - Long.BYTES];
                System.arraycopy(body, 0, newBody, 0, newBody.length);

                result = new Response(Integer.toString(tmp.getStatus()), newBody);
            }
        } catch (Exception e) {
            logger.error("Error in merging responses {}", responses, e);
            result = UtilResponses.serviceUnavailableResponse();
        }

        return result;
    }

    private Response askHttpClient(Request request, String id, Integer httpClientId) {
        Response response;
        if (httpClientId == this.id) {
            response = handleEntity(request, id, true);
        } else {
            try {
                response = clients[httpClientId].invoke(request);
            } catch (Exception e) {
                logger.error("Something wrong on request httpClient {}, request: {}, topology: {}",
                        clients[httpClientId], request, topology, e);
                response = UtilResponses.serviceUnavailableResponse();
            }
        }
        return response;
    }

    //assume there is a timestamp here, or it is 404 response with no data
    private long parseTimeStamp(Response response) {
        byte[] body = response.getBody();
        long result = -1;
        if (body.length >= Long.BYTES) {
            try {
                ByteBuffer wrap = ByteBuffer.wrap(body, body.length - Long.BYTES, Long.BYTES);
                result = wrap.getLong();
            } catch (Exception e) {
                logger.error("Error while parsing timestamp. {}", response, e);
            }
        }
        return result;
    }

    private boolean isConfirm(Response response) {
        int status = response.getStatus();
        return (status == 200) || (status == 201) || (status == 202) || (status == 404);
    }

    @Override
    public void handleDefault(
            final Request request,
            final HttpSession session) throws IOException {
        session.sendResponse(UtilResponses.badRequest());
    }

    private Response get(@Param(value = "id", required = true) String id, boolean proxyRequest) {
        ByteBuffer fromKey = wrapString(id);
        ByteBuffer toKey = DAO.nextKey(fromKey);

        Iterator<Record> range = dao.range(fromKey, toKey, proxyRequest);
        if (!range.hasNext()) {
            return UtilResponses.notFoundResponse();
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
                ok.addHeader(TOMBSTONE_HEADER);
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
        return UtilResponses.createdResponse();
    }

    private long timeStamp() {
        return System.currentTimeMillis();
    }

    private Response delete(@Param(value = "id", required = true) String id) {
        ByteBuffer key = wrapString(id);
        dao.upsert(Record.tombstone(key, timeStamp()));

        return UtilResponses.acceptedResponse();
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
                response = UtilResponses.badRequest();
                break;
        }
        return response;
    }
}
