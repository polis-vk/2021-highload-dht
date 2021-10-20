package ru.mail.polis.service.igorsamokhin;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import one.nio.util.Utf8;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ServiceImpl extends HttpServer implements Service {
    private static final String ENDPOINT_V0_STATUS = "/v0/status";
    private static final String ENDPOINT_V0_ENTITY = "/v0/entity";

    public static final String BAD_ID_RESPONSE = "Bad id";
    public static final int CAPACITY = 128;
    public static final int MAXIMUM_POOL_SIZE = 32;
    public static final int CORE_POOL_SIZE = 4;
    public static final int KEEP_ALIVE_TIME_MINUTES = 10;

    private final DAO dao;
    private boolean isWorking; //false by default

    private final ThreadPoolExecutor executor =
            new ThreadPoolExecutor(CORE_POOL_SIZE,
                    MAXIMUM_POOL_SIZE,
                    KEEP_ALIVE_TIME_MINUTES,
                    TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(CAPACITY));

    public ServiceImpl(int port, DAO dao) throws IOException {
        super(from(port));
        this.dao = dao;
    }

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {
        if (!isWorking) {
            return;
        }

        try {
            executor.execute(() -> {
                try {
                    Response response = invokeHandler(request);
                    session.sendResponse(response);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (RejectedExecutionException e) {
            session.sendResponse(UtilResponses.serviceUnavailableRequest());
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
        super.stop();
    }

    private static HttpServerConfig from(int port) {
        HttpServerConfig config = new HttpServerConfig();
        AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        acceptor.reusePort = true;

        config.acceptors = new AcceptorConfig[]{acceptor};
        return config;
    }

    private Response status() {
        return Response.ok("I'm OK");
    }

    @Override
    public void handleDefault(
            final Request request,
            final HttpSession session) throws IOException {
        session.sendResponse(UtilResponses.badRequest());
    }

    private Response get(@Param(value = "id", required = true) String id) {
        if (id.isBlank()) {
            return UtilResponses.badRequest(BAD_ID_RESPONSE);
        }

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
        if (id.isBlank()) {
            return UtilResponses.badRequest(BAD_ID_RESPONSE);
        }

        Record record = Record.of(wrapString(id), ByteBuffer.wrap(request.getBody()));
        try {
            dao.upsert(record);
        } catch (RuntimeException e) {
            return UtilResponses.serviceUnavailableRequest();
        }
        return UtilResponses.createdResponse();
    }

    private Response delete(@Param(value = "id", required = true) String id) {
        if (id.isBlank()) {
            return UtilResponses.badRequest(BAD_ID_RESPONSE);
        }

        ByteBuffer key = wrapString(id);
        try {
            dao.upsert(Record.tombstone(key));
        } catch (RuntimeException e) {
            return UtilResponses.serviceUnavailableRequest();
        }
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

    private Response invokeHandler(Request request) {
        Response response;
        try {
            String path = request.getPath();
            String id = request.getParameter("id=");
            if (path.equals(ENDPOINT_V0_STATUS)) {
                response = status();
            } else if (path.equalsIgnoreCase(ENDPOINT_V0_ENTITY) && (id != null)) {
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
            } else {
                response = UtilResponses.badRequest();
            }
        } catch (Exception e) {
            response = UtilResponses.serviceUnavailableRequest();
        }

        return response;
    }
}
