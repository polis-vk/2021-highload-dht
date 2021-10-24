package ru.mail.polis.service.asadullin_bulat;

import one.nio.http.HttpServer;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class BaseService extends HttpServer implements Service {

    private static final String INVALID_METHOD_MESSAGE = "Invalid method";
    private static final String OK_MESSAGE_MESSAGE = "Ok";
    private static final String BAD_ID_MESSAGE = "Bad id";
    private final DAO dao;

    public BaseService(int port, DAO dao) throws IOException {
        super(ServiceController.from(port));
        this.dao = dao;
    }

    @Path("/v0/status")
    public Response status() {
        return Response.ok(OK_MESSAGE_MESSAGE);
    }

    @Path("/v0/entity")
    public Response entity(Request request, @Param(value = "id", required = true) String id) {
        if (id.isBlank()) {
            return new Response(Response.BAD_REQUEST, BAD_ID_MESSAGE.getBytes(StandardCharsets.UTF_8));
        }
        int method = request.getMethod();
        switch (method) {
            case Request.METHOD_GET:
                return get(id);
            case Request.METHOD_PUT:
                return put(id, request.getBody());
            case Request.METHOD_DELETE:
                return delete(id);
            default:
                return new Response(
                        Response.METHOD_NOT_ALLOWED,
                        INVALID_METHOD_MESSAGE.getBytes(StandardCharsets.UTF_8)
                );
        }
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    private Response delete(String id) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        dao.upsert(Record.tombstone(key));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private Response put(String id, byte[] body) {
        ByteBuffer wrapKey = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        ByteBuffer wrapBody = ByteBuffer.wrap(body);
        dao.upsert(Record.of(wrapKey, wrapBody));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response get(String id) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        Iterator<Record> range = dao.range(key, DAO.nextKey(key));
        if (range.hasNext()) {
            Record next = range.next();
            return new Response(Response.OK, toByteArray(next.getValue()));
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private byte[] toByteArray(ByteBuffer buffer) {
        byte[] res = new byte[buffer.remaining()];
        buffer.get(res);
        return res;
    }
}
