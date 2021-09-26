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

    public Response get(String id) {

        ByteBuffer fromKey = Utils.stringToBytebuffer(id);

        Iterator<Record> range = dao.range(fromKey, DAO.nextKey(fromKey));

        if (range.hasNext()) {
            Record record = range.next();

            return new Response(Response.OK, Utils.bytebufferToBytes(record.getValue()));
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    public Response delete(String id) {

        dao.upsert(Record.tombstone(Utils.stringToBytebuffer(id)));

        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    public Response put(String id, Request request) {

        byte[] body = request.getBody();
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        ByteBuffer value = ByteBuffer.wrap(body);

        dao.upsert(Record.of(key, value));

        return new Response(Response.CREATED, Response.EMPTY);
    }
}
