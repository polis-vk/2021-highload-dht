package ru.mail.polis.service.gasparyansokrat;

import one.nio.http.Request;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

public class ServiceDAO {

    private final DAO refDao;
    private static final Logger LOG = LoggerFactory.getLogger(ServiceDAO.class);

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
                    return get(id);
                case Request.METHOD_PUT:
                    return put(id, request.getBody());
                case Request.METHOD_DELETE:
                    return delete(id);
                default:
                    return new Response(Response.METHOD_NOT_ALLOWED, "Bad request".getBytes(StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            throw new IOException("Error access DAO", e);
        }
    }

    public CompletableFuture<Response> asyncHandleRequest(final String id, final Request request) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                switch (request.getMethod()) {
                    case Request.METHOD_GET:
                        return get(id);
                    case Request.METHOD_PUT:
                        return put(id, request.getBody());
                    case Request.METHOD_DELETE:
                        return delete(id);
                    default:
                        return new Response(Response.METHOD_NOT_ALLOWED, ServiceImpl.BAD_REQUEST);
                }
            } catch (IOException e) {
                LOG.error("Error access DAO {}", e.getMessage());
                return new Response(Response.GATEWAY_TIMEOUT, ServiceImpl.BAD_REQUEST);
            }
        });

    }
}
