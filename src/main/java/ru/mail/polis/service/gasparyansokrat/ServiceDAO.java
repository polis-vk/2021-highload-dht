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

    ServiceDAO(final int cacheCapacity, DAO dao) {
        this.refDao = dao;
    }

    private byte[] cvtByteArray2Bytes(final ByteBuffer bf) {
        byte[] tmpBuff = new byte[bf.remaining()];
        bf.get(tmpBuff);
        return tmpBuff;
    }

    protected Response get(final String id) throws IOException {
        ByteBuffer start = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        Response resp = null;
        Record res = null;

        boolean accept = false;

        Iterator<Record> it = refDao.range(start, null); // DAO.nextKey(start)

        while (it.hasNext() && !accept) {
            res = it.next();
            accept = res.getKey().equals(start);
        }

        if (accept) {
            resp = new Response(Response.OK, cvtByteArray2Bytes(res.getValue()));
        } else {
            resp = new Response(Response.NOT_FOUND, Response.EMPTY);
        }

        return resp;
    }

    protected Response put(final String id, final byte[] data) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        ByteBuffer payload = ByteBuffer.wrap(data);
        Record temp = Record.of(key, payload);
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
    public Response handleRequest(Request req, final String id) throws IOException {

        Response resp = null;
        try {
            final int typeHttpMethod = req.getMethod();
            switch (typeHttpMethod) {
                case Request.METHOD_GET:
                    resp = this.get(id);
                    break;
                case Request.METHOD_PUT:
                    resp = this.put(id, req.getBody());
                    break;
                case Request.METHOD_DELETE:
                    resp = this.delete(id);
                    break;
                default:
                    resp = new Response(Response.METHOD_NOT_ALLOWED, "Bad request".getBytes(StandardCharsets.UTF_8));
                    break;
            }
        } catch (IOException e) {
            throw new IOException("Error access DAO", e);
        }
        return resp;
    }
}
