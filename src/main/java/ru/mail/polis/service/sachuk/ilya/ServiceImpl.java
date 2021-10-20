package ru.mail.polis.service.sachuk.ilya;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.io.UncheckedIOException;

public class ServiceImpl extends HttpServer implements Service {
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String STATUS_PATH = "/v0/status";

    private final EntityRequestHandler entityRequestHandler;
    private final RequestPoolExecutor requestPoolExecutor = new RequestPoolExecutor(
            new ExecutorConfig(16, 300)
    );

    public ServiceImpl(int port, DAO dao) throws IOException {
        super(configFrom(port));

        this.entityRequestHandler = new EntityRequestHandler(dao);
    }

    private static HttpServerConfig configFrom(int port) {
        HttpServerConfig httpServerConfig = new HttpServerConfig();
        AcceptorConfig acceptorConfig = new AcceptorConfig();

        acceptorConfig.port = port;
        acceptorConfig.reusePort = true;

        httpServerConfig.acceptors = new AcceptorConfig[]{acceptorConfig};

        return httpServerConfig;
    }

    private Response entityRequest(Request request, String id) {

        if (id == null || id.isBlank()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }

        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return entityRequestHandler.get(id);
            case Request.METHOD_PUT:
                return entityRequestHandler.put(id, request);
            case Request.METHOD_DELETE:
                return entityRequestHandler.delete(id);
            default:
                return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
        }
    }

    private Response status() {
        return new Response(Response.OK, Response.EMPTY);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    @Override
    public void handleRequest(Request request, HttpSession session) {
        String path = request.getPath();

        if (requestPoolExecutor.isQueueFull()) {
            requestPoolExecutor.executeNow(() -> {
                try {
                    session.sendResponse(new Response(Response.SERVICE_UNAVAILABLE));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            return;
        }

        switch (path) {
            case ENTITY_PATH:
                requestPoolExecutor.addTask(() -> {
                    String id = request.getParameter("id=");
                    Response response = entityRequest(request, id);

                    try {
                        session.sendResponse(response);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
                break;
            case STATUS_PATH:
                requestPoolExecutor.addTask(() -> {
                    Response response = status();
                    try {
                        session.sendResponse(response);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
                break;
            default:
                requestPoolExecutor.addTask(() -> {
                    try {
                        handleDefault(request, session);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
        }
    }
}
