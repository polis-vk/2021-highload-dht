package ru.mail.polis.service.gasparyansokrat;

import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;

public class ServiceImpl extends HttpServer implements Service {

    ServiceDAO servDAO;

    public ServiceImpl(final int port, final DAO dao) throws IOException {
        super(HttpConfigFactory.buildHttpConfig(port, "localhost"));
        this.servDAO = new ServiceDAO(8192, dao);
    }

    @Path("/v0/status")
    public Response status() {
        return Response.ok("OK");
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    /**
     *
     * @param req
     * @param id
     * @return
     */
    @Path("/v0/entity")
    public Response handleEntity(Request req,
                                @Param(value = "id", required = true) String id) throws IOException {
        Response resp = null;

        try {

            if (id.isEmpty()) {
                resp = new Response(Response.BAD_REQUEST, Response.EMPTY);
            } else {
                resp = servDAO.handleRequest(req, id);
            }

        } catch (IOException e) {
            throw new IOException("Bad request", e);
        }

        return resp;
    }
}
