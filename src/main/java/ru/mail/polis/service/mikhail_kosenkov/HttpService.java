package ru.mail.polis.service.mikhail_kosenkov;

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
import java.util.concurrent.atomic.AtomicReference;

public class HttpService extends HttpServer implements Service {

    private final DAOService daoService;

    public HttpService(int port, DAO dao) throws IOException {
        super(createConfig(port));
        daoService = new DAOService(dao);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }

    @Path("/v0/status")
    public Response handleStatus() {
        return Response.ok("Server is running");
    }

    @Path("/v0/entity")
    public Response handleEntity(Request request,
                                 @Param(value = "id", required = true) String id) {
        if (id == null || id.length() == 0) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return get(id);
            case Request.METHOD_PUT:
                return put(request, id);
            case Request.METHOD_DELETE:
                return delete(id);
            default:
                return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
        }
    }

    private Response delete(String id) {
        daoService.deleteEntity(id);
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private Response put(Request request, String id) {
        daoService.putEntity(id, request.getBody());
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response get(String id) {
        AtomicReference<Response> response = new AtomicReference<>();
        daoService.getEntity(id)
                .ifPresentOrElse(
                        bytes -> response.set(Response.ok(bytes)),
                        () -> response.set(new Response(Response.NOT_FOUND, Response.EMPTY)));
        return response.get();
    }

    private static HttpServerConfig createConfig(int port) {
        AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        acceptorConfig.reusePort = true;

        HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{acceptorConfig};
        return config;
    }
}
