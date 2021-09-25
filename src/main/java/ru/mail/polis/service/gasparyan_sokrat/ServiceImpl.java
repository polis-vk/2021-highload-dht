package ru.mail.polis.service.gasparyan_sokrat;

import one.nio.http.*;
import one.nio.util.Utf8;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ServiceImpl extends HttpServer implements Service  {

    ServiceDAO sDAO;

    public ServiceImpl(final int port, final DAO dao) throws IOException {
        super(HttpConfigFactory.buildHttpConfig(port, "127.0.0.1"));
        this.sDAO = new ServiceDAO(4096, dao);
    }

    @Override
    public void start() {
        super.start();
    }

    @Override
    public void stop() {
        super.stop();
    }


    @Path("/v0/status")
    public Response status() {
        return Response.ok("OK");
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    @Path("/v0/entity")
    public Response handleEntity(Request req,
                                @Param(value = "id", required = true) String ID) throws IOException
    {
        Response resp = null;
        try{
            if (ID.isEmpty())
                resp = new Response(Response.BAD_REQUEST, Response.EMPTY);
            else{
                resp = sDAO.handleRequest(req, ID);
            }
        } catch (Exception e) {
            throw new IOException("Bad request", e);
        }


        return resp;
    }
}
