package ru.mail.polis.request;

import one.nio.http.*;
import one.nio.pool.PoolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.ClusterProxySystem;
import ru.mail.polis.controller.MainController;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DefaultRequestHandler implements RequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRequestHandler.class);

    private static final int THREADS_AMOUNT = 3;
    private static final int REQUESTS_QUEUE_SIZE = 100;

    private final MainController controller;
    private final ClusterProxySystem clusterProxySystem;

    private final ExecutorService executorService = new ThreadPoolExecutor(THREADS_AMOUNT, THREADS_AMOUNT,
            0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>(REQUESTS_QUEUE_SIZE));

    public DefaultRequestHandler(MainController controller, ClusterProxySystem clusterProxySystem) {
        this.controller = controller;
        this.clusterProxySystem = clusterProxySystem;
    }

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {
        try {
            executorService.execute(() -> {
                Response response = processRequest(request);
                sendResponse(session, response);
            });
        } catch (RejectedExecutionException e) {
            LOG.info("Failed to add new request to executorService: {}", request, e);
            session.sendResponse(new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY));
        }
    }

    private void sendResponse(HttpSession session, Response response) {
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            LOG.info("Failed to send response: {}", response);
        }
    }

    private Response processRequest(Request request) {
        switch (request.getPath()) {
            case "/v0/entity":
                String idParam = "id=";
                String value = request.getParameter(idParam);
                if (value == null || value.isBlank()) {
                    LOG.warn("Value is blank");
                    return new Response(Response.BAD_REQUEST, "Id can't be blank".getBytes(StandardCharsets.UTF_8));
                }
                try {
                    String isRequestProxied = request.getHeader("Proxied:");
                    if (isRequestProxied != null && isRequestProxied.equals("true")) {
                        return controller.entity(value, request);
                    } else {
                        return clusterProxySystem.invokeEntityRequest(value, request);
                    }
                } catch (HttpException | IOException | PoolException e) {
                    LOG.warn("Failed for unknown reason while processing request", e);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY);
            case "/v0/status":
                return controller.status(request);
            default:
                return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
    }
}
