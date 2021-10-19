package ru.mail.polis.request;

import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.RequestHandler;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.controller.MainController;
import ru.mail.polis.lsm.artem_drozdov.DAOState;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DefaultRequestHandler implements RequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRequestHandler.class);

    private static final int REQUESTS_QUEUE_CAPACITY = 10_000;
    private static final int THREADS_AMOUNT = 10;

    private final MainController controller;
    private final BlockingQueue<RequestDTO> requestsQueue = new ArrayBlockingQueue<>(REQUESTS_QUEUE_CAPACITY, true);
    private final ExecutorService executorService = Executors.newFixedThreadPool(THREADS_AMOUNT);

    public DefaultRequestHandler(MainController controller) {
        this.controller = controller;
    }

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {
        DAOState daoState = controller.getDaoState();
        if (daoState != DAOState.OK) {
            session.sendResponse(new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY));
            LOG.warn("Server rejected response from: {} because of DAOState: {}",
                    session.getRemoteHost(), daoState);
            return;
        }
        executorService.execute(() -> {
            Response response = processRequest(request);
            try {
                session.sendResponse(response);
            } catch (IOException e) {
                LOG.warn("Failed to send response: {}", response);
            }
        });
    }

    Response processRequest(Request request) {
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
