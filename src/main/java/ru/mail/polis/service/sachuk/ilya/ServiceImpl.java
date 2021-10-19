package ru.mail.polis.service.sachuk.ilya;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ServiceImpl extends HttpServer implements Service {
    private Logger logger = LoggerFactory.getLogger(ServiceImpl.class);

    private static final String ENTITY_PATH = "/v0/entity";
    private static final String STATUS_PATH = "/v0/status";

    private final EntityRequestHandler entityRequestHandler;
    private final RequestPoolExecutor requestPoolExecutor = new RequestPoolExecutor(new ExecutorConfig());
    private final ExecutorService executorService = Executors.newFixedThreadPool(4);

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

    public Response entityRequest(Request request, String id) {

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

    public Response status() {
        return new Response(Response.OK, Response.EMPTY);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {

        RequestTask requestTask = new RequestTask(request, session);


        String path = request.getPath();
        logger.info(path);

        switch (path) {
            case ENTITY_PATH:
                String id = request.getParameter("id");
                executorService.execute(() -> {
                    Response response = entityRequest(request, id);
                    try {
                        session.sendResponse(response);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
                logger.info(id);
                break;
            case STATUS_PATH:
                executorService.execute(() -> {
                    Response response = status();
                    try {
                        session.sendResponse(response);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
                break;
            default:
                executorService.execute(() -> {
                    try {
                        handleDefault(request, session);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
        }
    }
}