package ru.mail.polis.service.ilia;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class ServiceImpl extends HttpServer implements Service {

    private final DAO dao;

    public ServiceImpl(
            int port,
            DAO dao
    ) throws IOException {
        super(connect(port));
        this.dao = dao;
    }

    private static HttpServerConfig connect(int port) {
        HttpServerConfig config = new HttpServerConfig();
        AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        acceptor.reusePort = true;
        config.acceptors = new AcceptorConfig[]{acceptor};
        return config;
    }

    @Path("/v0/status")
    public Response status() {
        return Response.ok("I'm ok");
    }

    /**
     * Processing and executing the request.
     *
     * @param request - http request
     * @param id - entity id
     * @return http response
     */
    @Path("/v0/entity")
    public Response entity(
            Request request,
            @Param(value = "id", required = true) String id
    ) {
        if (id.isBlank()) {
            return new Response(Response.BAD_REQUEST, "Request is empty".getBytes(StandardCharsets.UTF_8));
        }
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return get(id);
            case Request.METHOD_PUT:
                return put(id, request.getBody());
            case Request.METHOD_DELETE:
                return delete(id);
            default:
                return new Response(Response.METHOD_NOT_ALLOWED, "Unknown request".getBytes(StandardCharsets.UTF_8));
        }
    }

    private Response delete(String id) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        dao.upsert(Record.tombstone(key));
        return new Response(Response.ACCEPTED, "Delete is done".getBytes(StandardCharsets.UTF_8));
    }

    private Response put(String id, byte[] body) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        ByteBuffer value = ByteBuffer.wrap(body);
        dao.upsert(Record.of(key, value));
        return new Response(Response.CREATED, "Put is done".getBytes(StandardCharsets.UTF_8));
    }

    private Response get(String id) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        Iterator<Record> iterator = dao.range(key, DAO.nextKey(key));
        if (iterator.hasNext()) {
            Record record = iterator.next();
            ByteBuffer value = record.getValue();
            byte[] result = new byte[value.remaining()];
            value.get(result);
            return new Response(Response.OK, result);
        } else {
            return new Response(Response.NOT_FOUND, "No entry found for this id".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }
}
