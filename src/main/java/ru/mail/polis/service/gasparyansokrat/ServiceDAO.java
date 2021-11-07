package ru.mail.polis.service.gasparyansokrat;

import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class ServiceDAO {

    private final DAO refDao;

    ServiceDAO(DAO dao) {
        this.refDao = dao;
    }

    protected Response get(final String id) throws IOException {
        ByteBuffer start = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        Iterator<Record> it = refDao.rangeWithTombstone(start, DAO.nextKey(start));

        if (it.hasNext()) {
            return new Response(Response.OK, it.next().getRawBytes());
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    protected Response put(final String id, final byte[] data) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        ByteBuffer payload = ByteBuffer.wrap(data);
        Record temp = Record.direct(key, payload);
        refDao.upsert(temp);
        return new Response(Response.CREATED, Response.EMPTY);
    }

    protected Response delete(final String id) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        Record temp = Record.tombstone(key);
        refDao.upsert(temp);
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    /**
     * some doc.
     */
    public Response handleRequest(final String id, final Request request) throws IOException {
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    return this.get(id);
                case Request.METHOD_PUT:
                    return this.put(id, request.getBody());
                case Request.METHOD_DELETE:
                    return this.delete(id);
                default:
                    return new Response(Response.METHOD_NOT_ALLOWED, "Bad request".getBytes(StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            throw new IOException("Error access DAO", e);
        }
    }
}
