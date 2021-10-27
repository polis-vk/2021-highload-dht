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
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ServiceImpl extends HttpServer implements Service {
    private static final String ENDPOINT_V0_STATUS = "/v0/status";
    private static final String ENDPOINT_V0_ENTITY = "/v0/entity";
    private final Logger logger = LoggerFactory.getLogger(ServiceImpl.class);

    public static final String BAD_ID_RESPONSE = "Bad id";
    public static final int CAPACITY = 4;
    public static final int MAXIMUM_POOL_SIZE = 8;
    public static final int CORE_POOL_SIZE = 4;
    public static final int KEEP_ALIVE_TIME_MINUTES = 10;

    private final DAO dao;
    private String[] topology;
    private HttpClient[] clients;
    private int id;
    private boolean isWorking; //false by default

    private final ThreadPoolExecutor executor =
            new ThreadPoolExecutor(CORE_POOL_SIZE,
                    MAXIMUM_POOL_SIZE,
                    KEEP_ALIVE_TIME_MINUTES,
                    TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(CAPACITY));

    public ServiceImpl(int port, DAO dao, Set<String> topology) throws IOException {
        super(from(port));
        this.dao = dao;
        this.topology = new String[topology.size()];
        this.clients = new HttpClient[topology.size()];

        Iterator<String> iterator = topology.iterator();
        for (int i = 0; iterator.hasNext(); i++) {
            String adr = iterator.next();
            if (adr.endsWith(Integer.toString(port))) {
                this.id = i;
            }
            this.topology[i] = adr;
            ConnectionString conn = new ConnectionString(adr + "?timeout=100");
            HttpClient client = new HttpClient(conn);
            this.clients[i] = client;
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

    private int getNodeId(String id) {
        if (id.isBlank()) {
            return -1;
        }

        int max = Integer.MIN_VALUE;
        int idMax = -1;
        for (int i = 0; i < topology.length; i++) {
            int hashCode = (topology[i] + id).hashCode();
            if (hashCode > max) {
                max = hashCode;
                idMax = i;
            }
        }

        return idMax;
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
            if (!executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.NANOSECONDS)) {
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
                       @Param(value = "id", required = true) String id) {
        executor.execute(() -> {
            Response response;
            try {
                int nodeId = getNodeId(id);
                if (nodeId == -1) {
                    response = UtilResponses.badRequest(BAD_ID_RESPONSE);
                } else if (nodeId == this.id) {
                    response = handleEntity(request, id);
                } else {
                    response = clients[nodeId].invoke(request);
                }
            } catch (RuntimeException e) {
                response = UtilResponses.serviceUnavailableResponse();
            } catch (PoolException e) {
                logger.error("Perhaps one of nodes in cluster is not responding\n Request:\n{} ", request, e);
                response = UtilResponses.serviceUnavailableResponse();
            } catch (Exception e) {
                logger.error("Exception in entity handling. Request:\n{} ", request, e);
                response = UtilResponses.serviceUnavailableResponse();
            }
            sendResponse(session, response);
        });
    }

    @Override
    public void handleDefault(
            final Request request,
            final HttpSession session) throws IOException {
        session.sendResponse(UtilResponses.badRequest());
    }

    private Response get(@Param(value = "id", required = true) String id) {
        ByteBuffer fromKey = wrapString(id);
        ByteBuffer toKey = DAO.nextKey(fromKey);

        Iterator<Record> range = dao.range(fromKey, toKey);
        if (!range.hasNext()) {
            return UtilResponses.notFoundResponse();
        }

        byte[] value = extractBytes(range.next().getValue());
        return Response.ok(value);
    }

    private Response put(@Param(value = "id", required = true) String id,
                         Request request) {
        Record record = Record.of(wrapString(id), ByteBuffer.wrap(request.getBody()));
        dao.upsert(record);
        return UtilResponses.createdResponse();
    }

    private Response delete(@Param(value = "id", required = true) String id) {
        ByteBuffer key = wrapString(id);
        dao.upsert(Record.tombstone(key));

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

    private Response handleEntity(Request request, String id) {
        Response response;
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                response = get(id);
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
