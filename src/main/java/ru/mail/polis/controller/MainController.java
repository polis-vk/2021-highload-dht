package ru.mail.polis.controller;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.RecordUtil;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

public class MainController implements Controller {

    private final DAO dao;

    public MainController(DAO dao) {
        this.dao = dao;
    }

    @SuppressWarnings("unused")
    @Path("/v0/status")
    public Response status(Request request) {
        return new Response(Response.OK, Response.EMPTY);
    }

    @SuppressWarnings("unused")
    @Path("/v0/entity")
    public Response entity(@Param(value = "id", required = true) String id,
                           Request request) {
        if (id.isBlank()) {
            return new Response(Response.BAD_REQUEST, "id can't be blank".getBytes(StandardCharsets.UTF_8));
        }
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return get(id);
            case Request.METHOD_PUT:
                return put(id, request.getBody());
            case Request.METHOD_DELETE:
                return delete(id);
            default:
                return new Response(Response.METHOD_NOT_ALLOWED,
                        "No such method allowed".getBytes(StandardCharsets.UTF_8));
        }
    }

    private Response get(final String id) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        Iterator<Record> iterator = dao.range(key, DAO.nextKey(key));

        Record record = null;
        if (iterator.hasNext()) {
            record = iterator.next();
        }
        return record == null
            ? new Response(Response.NOT_FOUND, Response.EMPTY)
            : new Response(Response.OK, RecordUtil.extractBytes(record.getValue()));
    }

    private Response put(final String id, final byte[] payload) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        ByteBuffer value = ByteBuffer.wrap(payload);
        dao.upsert(Record.of(key, value));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response delete(final String id) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        dao.upsert(Record.tombstone(key));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

}
