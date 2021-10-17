package ru.mail.polis.service.medalexey;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.artemdrozdov.LsmDAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ServiceImpl extends HttpServer implements Service {

    private static final String STATUS_PATH = "/v0/status";
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String ID_PARAMETER_KEY = "id";
    private static final int QUEUE_CAPACITY = 100;
    private final ThreadPoolExecutor executor;
    private final Logger logger;

    private final DAO dao;

    public ServiceImpl(final int port, final DAO dao, int threads) throws IOException {
        super(from(port));
        this.dao = dao;
        logger = LoggerFactory.getLogger(LsmDAO.class);
        this.executor = new ThreadPoolExecutor(threads, threads, 0L,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(QUEUE_CAPACITY));
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

    private void handleRequestTask(Request request, HttpSession session) {
        Response response;
        String requestPath = request.getPath();

        switch (requestPath) {
            case STATUS_PATH: {
                response = status();
                break;
            }
            case ENTITY_PATH: {
                String id = request.getParameter(ID_PARAMETER_KEY);
                response = entity(request, id);
                break;
            }
            default: {
                response = new Response(Response.BAD_REQUEST, Response.EMPTY);
            }
        }
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            logger.warn("Error sending response", e);
        }
    }

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {
        if (executor.getQueue().remainingCapacity() > 0) {
            synchronized (this) {
                if (executor.getQueue().remainingCapacity() > 0) {
                    executor.execute(() -> handleRequestTask(request, session));
                    return;
                }
            }
        }

        session.sendResponse(new Response(Response.INTERNAL_ERROR, Response.EMPTY));
    }

    /**
     *  Removed, upserted or returned entity depends on request method.
     *
     * @param request - http request
     * @param id entity id
     * @return http response (2xx if ok, 4xx if error)
     */
    public Response entity(final Request request, final String id) {
        if (id == null || id.isBlank() || id.equals("=")) {
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
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    @Override
    public synchronized void stop() {
        executor.shutdown();
        super.stop();
    }
}
