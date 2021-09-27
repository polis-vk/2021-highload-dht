package ru.mail.polis.service.danilaeremenko;

import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class BasicService extends HttpServer implements Service {
    private final DaoWrapper daoWrapper;

    public BasicService(int port, DAO dao) throws IOException {
        super(MyConfigFactory.fromPort(port));
        this.daoWrapper = new DaoWrapper(dao);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, "Not found".getBytes(StandardCharsets.UTF_8)));
    }

    @Path("/v0/status")
    public Response status() {
        return Response.ok("Okay bro");
    }

    @Path("/v0/entity")
    public Response entity(
            final Request request,
            @Param(value = "id", required = true) final String id
    ) {
        if (id.isBlank()) {
            return new Response(Response.BAD_REQUEST, "Undefined method".getBytes(StandardCharsets.UTF_8));
        }
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return this.daoWrapper.getEntity(id);
            case Request.METHOD_PUT:
                return this.daoWrapper.putEntity(id, request.getBody());
            case Request.METHOD_DELETE:
                return this.daoWrapper.deleteEntity(id);
            default:
                return new Response(Response.BAD_REQUEST, "Undefined method".getBytes(StandardCharsets.UTF_8));
        }

    }
}
