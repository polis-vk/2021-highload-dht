package ru.mail.polis.service.alexander_kuptsov;

import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class HttpRestService extends HttpServer implements Service {
    private static final Logger logger = LoggerFactory.getLogger(HttpRestService.class);

    private static final int THREADS_COUNT = 8;
    private static final int QUEUE_CAPACITY = 128;

    private final ThreadPoolExecutor executor = new ThreadPoolExecutor(
            THREADS_COUNT,
            THREADS_COUNT,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(QUEUE_CAPACITY)
    );

    private final DAO dao;

    private static final class RequestPath {
        public static final String STATUS = "/v0/status";
        public static final String ENTITY = "/v0/entity";

        private RequestPath() {
        }
    }

    private static final class RequestParameters {
        public static final String ID = "id";
        public static final String EMPTY_ID = "=";

        private RequestParameters() {
        }
    }

    public HttpRestService(final int port,
                           final DAO dao) throws IOException {
        super(HttpServiceUtils.createConfigByPort(port));
        this.dao = dao;
    }

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {
        synchronized (this) {
            try {
                executor.execute(() -> doHandleRequest(request, session));
            } catch (RejectedExecutionException e) {
                logger.error("Can't handle request", e);
                var unavailableResponse = new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY);
                session.sendResponse(unavailableResponse);
            }
        }
    }

    @Override
    public synchronized void stop() {
        executor.shutdown();
        try {
            final int waitTime = 5;
            if (!executor.awaitTermination(waitTime, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Can't await for termination");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        super.stop();
    }

    private void doHandleRequest(Request request, HttpSession session) {
        Response response;
        String requestPath = request.getPath();

        switch (requestPath) {
            case RequestPath.STATUS: {
                response = status();
                break;
            }
            case RequestPath.ENTITY: {
                response = entity(request);
                break;
            }
            default: {
                response = handleDefaultRequest();
                break;
            }
        }
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            logger.error("Can't send response", e);
        }
    }

    /**
     * This method is part of HTTP REST API protocol. Implements status check.
     *
     * @return HTTP code 200
     */
    private Response status() {
        return Response.ok("OK");
    }

    /**
     * This method is part of HTTP REST API protocol. Implements:
     * <pre>
     * HTTP GET /v0/entity?id=ID -- get data by given key
     * HTTP PUT /v0/entity?id=ID -- upsert data by given key
     * HTTP DELETE /v0/entity?id=ID -- delete data by given key
     * </pre>
     *
     * @param request {@link Request}
     * @return HTTP code 200 with data
     * HTTP code 201
     * HTTP code 202
     * HTTP code 400
     * HTTP code 404
     * HTTP code 405
     */
    private Response entity(final Request request) {
        String id = request.getParameter(RequestParameters.ID);
        if (id == null || id.equals(RequestParameters.EMPTY_ID)) {
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
                return new Response(
                        Response.METHOD_NOT_ALLOWED,
                        "Wrong method. Try GET/PUT/DELETE".getBytes(StandardCharsets.UTF_8)
                );
        }
    }

    private Response handleDefaultRequest() {
        return new Response(Response.BAD_REQUEST, Response.EMPTY);
    }

    /**
     * Implements HTTP GET /v0/entity?id=ID -- get data by given key.
     *
     * @param id data key
     * @return HTTP code 200 with data
     * HTTP code 404
     */
    private Response get(String id) {
        final ByteBuffer key = HttpServiceUtils.wrapIdToBuffer(id);
        final Iterator<Record> range = dao.range(key, DAO.nextKey(key));
        if (range.hasNext()) {
            final Record first = range.next();
            return new Response(Response.OK, HttpServiceUtils.extractBytes(first.getValue()));
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    /**
     * Implements HTTP PUT /v0/entity?id=ID -- upsert data by given key.
     *
     * @param id   data key
     * @param body array of bytes with given data
     * @return HTTP code 201
     */
    private Response put(String id, byte[] body) {
        final ByteBuffer key = HttpServiceUtils.wrapIdToBuffer(id);
        final ByteBuffer value = ByteBuffer.wrap(body);
        dao.upsert(Record.of(key, value));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    /**
     * Implements HTTP DELETE /v0/entity?id=ID -- delete data by given key.
     *
     * @param id data key
     * @return HTTP code 202
     */
    private Response delete(String id) {
        final ByteBuffer key = HttpServiceUtils.wrapIdToBuffer(id);
        dao.upsert(Record.tombstone(key));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }
}
