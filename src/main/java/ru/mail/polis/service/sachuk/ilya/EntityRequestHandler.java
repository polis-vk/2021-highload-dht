package ru.mail.polis.service.sachuk.ilya;

import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.Utils;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class EntityRequestHandler {

    private final DAO dao;

    public EntityRequestHandler(DAO dao) {
        this.dao = dao;
    }

    public Response handle(Request request, String id) {
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return get(id);
            case Request.METHOD_PUT:
                return put(id, request);
            case Request.METHOD_DELETE:
                return delete(id);
            default:
                return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
        }
    }

    private Response get(String id) {

        ByteBuffer fromKey = Utils.stringToBytebuffer(id);

        Iterator<Record> range = dao.range(fromKey, DAO.nextKey(fromKey));

        if (range.hasNext()) {
            Record record = range.next();

            return new Response(Response.OK, Utils.bytebufferToBytes(record.getValue()));
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private Response delete(String id) {

        dao.upsert(Record.tombstone(Utils.stringToBytebuffer(id), Utils.timeStampToByteBuffer(System.currentTimeMillis())));

        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private Response put(String id, Request request) {

        byte[] body = request.getBody();
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        ByteBuffer value = ByteBuffer.wrap(body);
//TODO передавать timestamp
        dao.upsert(Record.of(key, value, Utils.timeStampToByteBuffer(System.currentTimeMillis())));

        return new Response(Response.CREATED, Response.EMPTY);
    }
}
