package ru.mail.polis.service.shabinsky_dmitry;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class BasicService extends HttpServer implements Service {

    public final static String STATUS_PATH = "/v0/status";
    public final static String ENTITY_PATH = "/v0/entity";

    private final Response badResponse = new Response(Response.BAD_REQUEST, Response.EMPTY);
    private final Response unavailableResponse = new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY);

    private final DAO dao;
    private final BlockingQueue<Runnable> workQueue;
    private final int lengthQueue;
    private final ExecutorService executor;

    public BasicService(final int port, final DAO dao, final int lengthWorkQueue) throws IOException {
        super(from(port));
        this.dao = dao;

        int coreSize = Runtime.getRuntime().availableProcessors();
        this.lengthQueue = lengthWorkQueue;
        this.workQueue = new LinkedBlockingQueue<>(lengthQueue);
        this.executor = new ThreadPoolExecutor(
            coreSize,
            coreSize,
            0L,
            TimeUnit.MILLISECONDS,
            this.workQueue
        );
    }

    private static HttpServerConfig from(final int port) {
        final HttpServerConfig config = new HttpServerConfig();
        final AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        acceptor.reusePort = true;
        config.acceptors = new AcceptorConfig[]{acceptor};
        return config;
    }

    public Response status() {
        return Response.ok("I'm ok");
    }

    public Response entity(
        final Request request,
        @Param(value = "id", required = true) final String id
    ) {
        if (id.isBlank()) {
            return new Response(Response.BAD_REQUEST, "Bad id".getBytes(StandardCharsets.UTF_8));
        }

        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return get(id);
            case Request.METHOD_PUT:
                return put(id, request.getBody());
            case Request.METHOD_DELETE:
                return delete(id);
            default:
                return new Response(Response.METHOD_NOT_ALLOWED, "Wrong method".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public void handleRequest(Request request, HttpSession session) {
        switch (request.getPath()) {

            //without exec block
            case STATUS_PATH:
                sendResponse(session, status());
                break;

            // with exec block
            case ENTITY_PATH:
                if (isQueueFull()) {
                    sendResponse(session, unavailableResponse);
                    break;
                }

                String id = request.getParameter("id=");
                if (id == null) {
                    sendResponse(session, badResponse);
                    break;
                }

                executor.execute(() -> sendResponse(session, entity(request, id)));
                break;

            default:
                sendResponse(session, badResponse);
        }
    }

    private boolean isQueueFull() {
        return workQueue.size() >= lengthQueue;
    }

    private void sendResponse(HttpSession session, Response response) {
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void handleDefault(
        Request request,
        HttpSession session
    ) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    private Response delete(String id) {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        dao.upsert(Record.tombstone(key));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private Response put(String id, byte[] body) {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final ByteBuffer value = ByteBuffer.wrap(body);
        dao.upsert(Record.of(key, value));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private static byte[] extractBytes(final ByteBuffer buffer) {
        final byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    private Response get(String id) {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final Iterator<Record> range = dao.range(key, DAO.nextKey(key));
        if (range.hasNext()) {
            final Record first = range.next();
            return new Response(Response.OK, extractBytes(first.getValue()));
        }
        return new Response(Response.NOT_FOUND, Response.EMPTY);
    }
}
