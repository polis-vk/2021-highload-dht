package ru.mail.polis.request;

import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.RequestHandler;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.controller.MainController;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DefaultRequestHandler implements RequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRequestHandler.class);

    private static final int CORES_SIZE = Runtime.getRuntime().availableProcessors();
    private static final int REQUESTS_QUEUE_SIZE = 100;

    private final MainController controller;

    private final ExecutorService executorService = new ThreadPoolExecutor(CORES_SIZE, CORES_SIZE + CORES_SIZE / 2,
            0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>(REQUESTS_QUEUE_SIZE));

    public DefaultRequestHandler(MainController controller) {
        this.controller = controller;
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
                return controller.entity(value, request);
            case "/v0/status":
                return controller.status(request);
            default:
                return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
    }
}
