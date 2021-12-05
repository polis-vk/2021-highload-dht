package ru.mail.polis.service.gasparyansokrat;

import one.nio.http.Request;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.http.HttpResponse;
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

    protected Iterator<Record> getRange(final String startKey, final String endKey) {
        ByteBuffer start = ByteBuffer.wrap(startKey.getBytes(StandardCharsets.UTF_8));
        ByteBuffer end = endKey.isEmpty() ? null : ByteBuffer.wrap(endKey.getBytes(StandardCharsets.UTF_8));
        Iterator<Record> rangeIt = refDao.range(start, end);
        return rangeIt;
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

    public CompletableFuture<HttpResponse<byte[]>> asyncHandleRequest(final String id,
                                                                      final RequestParameters params) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                switch (params.getHttpMethod()) {
                    case Request.METHOD_GET:
                        final Response getResponse = get(id);
                        return DummyResponses.buildResponse(getResponse.getStatus(), getResponse.getBody());
                    case Request.METHOD_PUT:
                        final Response putResponse = put(id, params.getBodyRequest());
                        return DummyResponses.buildResponse(putResponse.getStatus(), putResponse.getBody());
                    case Request.METHOD_DELETE:
                        final Response delResponse = delete(id);
                        return DummyResponses.buildResponse(delResponse.getStatus(), delResponse.getBody());
                    default:
                        return DummyResponses.buildResponse(HttpURLConnection.HTTP_BAD_METHOD, new byte[0]);
                }
            } catch (IOException e) {
                LOG.error("Error access DAO {}", e.getMessage());
                return DummyResponses.buildResponse(HttpURLConnection.HTTP_GATEWAY_TIMEOUT,
                                                        ServiceImpl.BAD_REQUEST.getBytes(StandardCharsets.UTF_8));
            }
        });

    }
}
