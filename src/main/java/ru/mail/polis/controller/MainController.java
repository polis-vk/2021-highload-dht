package ru.mail.polis.controller;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

public class MainController implements Controller {

    private final DAO dao;

    public MainController(DAO dao) {
        this.dao = dao;
    }

    @Path("/v0/status")
    public Response status() {
        return Response.ok(Response.EMPTY);
    }

    @Path("/v0/entity")
    public Response entity(@Param("id") String id,
                            Request request) {
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
        Iterator<Record> iterator = dao.range(key, key);

        Record record = null;
        if (iterator.hasNext()) {
            record = iterator.next();
        }

        return record != null && record.getKey().equals(key)
            ? Response.ok(new String(record.getValue().array()))
            : new Response(Response.NOT_FOUND);
    }

    private Response put(final String id, final byte[] payload) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        ByteBuffer value = ByteBuffer.wrap(payload);
        dao.upsert(Record.of(key, value));
        return new Response(Response.CREATED);
    }

    private Response delete(final String id) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        dao.upsert(Record.tombstone(key));
        return new Response(Response.ACCEPTED);
    }

}
