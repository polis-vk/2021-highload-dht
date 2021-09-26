package ru.mail.polis.service.sachuk.ilya;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;

public class ServiceImpl extends HttpServer implements Service {

    private final EntityRequestHandler entityRequestHandler;

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

    @Path(value = "/v0/entity")
    public Response entityRequest(
            Request request,
            @Param(value = "id", required = true) String id
    ) {

        if (id.isBlank()) {
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

    @Path(value = "/v0/status")
    public Response status() {
        return new Response(Response.OK, Response.EMPTY);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }
}
