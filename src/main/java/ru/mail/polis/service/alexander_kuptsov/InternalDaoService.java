package ru.mail.polis.service.alexander_kuptsov;

import one.nio.http.Response;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class InternalDaoService {

    private final DAO dao;

    public InternalDaoService(DAO dao) {
        this.dao = dao;
    }

    /**
     * Implements HTTP GET /v0/entity?id=ID -- get data by given key.
     *
     * @param id data key
     * @return HTTP code 200 with data
     *         HTTP code 404
     */
    public Response get(String id) {
        ByteBuffer key = HttpServiceUtils.wrapIdToBuffer(id);
        Iterator<Record> range = dao.range(key, DAO.nextKey(key));
        if (range.hasNext()) {
            Record first = range.next();
            return new Response(Response.OK, HttpServiceUtils.extractBytes(first.getValue()));
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    /**
     * Implements HTTP PUT /v0/entity?id=ID -- upsert data by given key.
     *
     * @param id   data key
     * @param body array of bytes with given data
     * @return HTTP code 201
     */
    public Response put(String id, byte[] body) {
        ByteBuffer key = HttpServiceUtils.wrapIdToBuffer(id);
        ByteBuffer value = ByteBuffer.wrap(body);
        dao.upsert(Record.of(key, value));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    /**
     * Implements HTTP DELETE /v0/entity?id=ID -- delete data by given key.
     *
     * @param id data key
     * @return HTTP code 202
     */
    public Response delete(String id) {
        ByteBuffer key = HttpServiceUtils.wrapIdToBuffer(id);
        dao.upsert(Record.tombstone(key));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }
}
