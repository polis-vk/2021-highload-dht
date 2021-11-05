package ru.mail.polis.service.gasparyansokrat;

import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;

public class ServiceDAO {

    private final DAO refDao;

    ServiceDAO(DAO dao) {
        this.refDao = dao;
    }

    public static byte[] cvtByteArray2Bytes(final ByteBuffer bf) {
        byte[] tmpBuff = new byte[bf.remaining()];
        bf.get(tmpBuff);
        return tmpBuff;
    }

    protected Response get(final String id) throws IOException {
        ByteBuffer start = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        Record res = null;

        boolean accept = false;

        Iterator<Record> it = refDao.range(start, DAO.nextKey(start));

        if (it.hasNext()) {
            res = it.next();
            accept = true;
        }

        Response resp;
        if (accept) {
            resp = new Response(Response.OK, cvtByteArray2Bytes(res.getRawValue()));
        } else {
            resp = new Response(Response.NOT_FOUND, Response.EMPTY);
        }

        return resp;
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
    public Response handleRequest(final Map<String, String> params,
                                  final Request request) throws IOException {
        try {
            final String id = params.get("id");
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
