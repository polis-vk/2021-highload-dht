package ru.mail.polis.service.alex;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.http.HttpSession;
import one.nio.http.Path;
import one.nio.http.Param;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class AlexServer extends HttpServer {

    private final DAO dao;

    public AlexServer(final HttpServerConfig config, final DAO dao) throws IOException {
        super(config);
        this.dao = dao;
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        final Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }

    @Path("/v0/status")
    public Response getStatus() {
        return Response.ok("Server running...");
    }

    @Path("/v0/entity")
    public Response entity(
            final Request request,
            @Param(value = "id", required = true) final String entityId) {

        if (entityId.isEmpty()) {
            return new Response(Response.BAD_REQUEST, "Empty ID!".getBytes(StandardCharsets.UTF_8));
        }

        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return getEntity(entityId);
            case Request.METHOD_PUT:
                return putEntity(entityId, request.getBody());
            case Request.METHOD_DELETE:
                return deleteEntity(entityId);
            default:
                return new Response(Response.METHOD_NOT_ALLOWED, "Wrong method!".getBytes(StandardCharsets.UTF_8));
        }
    }

    private Response getEntity(final String id) {
        final ByteBuffer key = byteBufferFrom(id);
        final Iterator<Record> iteratorRecord = dao.range(key, DAO.nextKey(key));
        if (iteratorRecord.hasNext()) {
            final Record record = iteratorRecord.next();
            return new Response(Response.OK, record.getValue().array());
        } else {
            return new Response(Response.NOT_FOUND, "Not found record!".getBytes(StandardCharsets.UTF_8));
        }
    }

    private Response putEntity(final String id, final byte[] data) {
        final ByteBuffer key = byteBufferFrom(id);
        final ByteBuffer value = ByteBuffer.wrap(data);
        dao.upsert(Record.of(key, value));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response deleteEntity(final String id) {
        final ByteBuffer key = byteBufferFrom(id);
        dao.upsert(Record.tombstone(key));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private static ByteBuffer byteBufferFrom(final String value) {
        return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
    }
}
