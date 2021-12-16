package ru.mail.polis.service.danilaeremenko;

import one.nio.http.Response;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

final class DaoWrapper {
    private final DAO dao;

    DaoWrapper(DAO dao) {
        this.dao = dao;
    }

    private byte[] brFromBuffer(ByteBuffer buff) {
        byte[] bytes = new byte[buff.remaining()];
        buff.get(bytes);
        return bytes;
    }

    public Response getEntity(String id) {
        final ByteBuffer idBuffer = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final Iterator<Record> range = this.dao.range(
                idBuffer,
                DAO.nextKey(idBuffer)
        );
        if (range.hasNext()) {
            final Record resRecord = range.next();
            return new Response(
                    Response.OK,
                    brFromBuffer(resRecord.getValue())
            );
        } else {
            return new Response(Response.NOT_FOUND, "Not Found".getBytes(StandardCharsets.UTF_8));
        }

    }

    public Response putEntity(String id, final byte[] body) {
        Record newRecord = Record.of(
                ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8)),
                ByteBuffer.wrap(body)
        );
        this.dao.upsert(newRecord);
        return new Response(Response.CREATED, "Created".getBytes(StandardCharsets.UTF_8));
    }

    public Response deleteEntity(String id) {
        dao.upsert(
                Record.tombstone(ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8)))
        );
        return new Response(Response.ACCEPTED, "Accepted".getBytes(StandardCharsets.UTF_8));
    }
}
