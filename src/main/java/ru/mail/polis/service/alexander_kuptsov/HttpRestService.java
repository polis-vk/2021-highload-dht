package ru.mail.polis.service.alexander_kuptsov;

import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
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

    private final RequestHandler requestHandler;

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

    private static final int[] ALLOWED_REQUEST_METHODS = new int[] {
            Request.METHOD_GET,
            Request.METHOD_PUT,
            Request.METHOD_DELETE
    };

    public HttpRestService(final int port, Set<String> topology, DAO dao) throws IOException {
        super(HttpServiceUtils.createConfigByPort(port));
        InternalDaoService internalDaoService = new InternalDaoService(dao);
        this.requestHandler = new RequestHandler(topology, port, internalDaoService);
    }

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {
        synchronized (this) {
            if (canExecuteRequest()) {
                executor.execute(() -> doHandleRequest(request, session));
                return;
            }
        }
        logger.info("Can't handle request");
        var unavailableResponse = new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY);
        session.sendResponse(unavailableResponse);
    }

    @Override
    public synchronized void stop() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
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

    private boolean canExecuteRequest() {
        return executor.getQueue().remainingCapacity() > 0;
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
     *         HTTP code 201
     *         HTTP code 202
     *         HTTP code 400
     *         HTTP code 404
     *         HTTP code 405
     *         HTTP code 502
     */
    private Response entity(final Request request) {
        String id = request.getParameter(RequestParameters.ID);
        if (id == null || id.equals(RequestParameters.EMPTY_ID)) {
            return new Response(Response.BAD_REQUEST, "Bad id".getBytes(StandardCharsets.UTF_8));
        }

        int requestMethod = request.getMethod();
        if (Arrays.stream(ALLOWED_REQUEST_METHODS).anyMatch(value -> value == requestMethod)) {
            return requestHandler.entity(id, requestMethod, request);
        }
        return new Response(
                Response.METHOD_NOT_ALLOWED,
                "Wrong method. Try GET/PUT/DELETE".getBytes(StandardCharsets.UTF_8)
        );
    }

    private Response handleDefaultRequest() {
        return new Response(Response.BAD_REQUEST, Response.EMPTY);
    }
}
