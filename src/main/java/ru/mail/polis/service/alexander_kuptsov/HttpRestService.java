package ru.mail.polis.service.alexander_kuptsov;

import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class HttpRestService extends HttpServer implements Service {
    private final DAO dao;

    public HttpRestService(final int port,
                           final DAO dao) throws IOException {
        super(HttpServiceUtils.createConfigByPort(port));
        this.dao = dao;
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    /**
     * This method is part of HTTP REST API protocol. Implements status check.
     *
     * @return HTTP code 200
     */
    @Path("/v0/status")
    public Response status() {
        return Response.ok("OK");
    }

    /**
     * This method is part of HTTP REST API protocol. Implements:
     * <pre>
     * HTTP GET /v0/entity?id=ID -- get data by given key
     * HTTP PUT /v0/entity?id=ID -- upsert data by given key
     * HTTP DELETE /v0/entity?id=ID -- delete data by given key
     * </pre>
     *
     * @param request {@link Request}
     * @param id      data key
     * @return HTTP code 200 with data
     *         HTTP code 201
     *         HTTP code 202
     *         HTTP code 400
     *         HTTP code 404
     *         HTTP code 405
     */
    @Path("/v0/entity")
    public Response entity(final Request request,
                           @Param(value = "id", required = true) final String id) {
        if (id.isBlank()) {
            return new Response(Response.BAD_REQUEST, "Bad id".getBytes(StandardCharsets.UTF_8));
        }
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return get(id);
            case Request.METHOD_PUT:
                return put(id, request.getBody());
            case Request.METHOD_DELETE:
                return delete(id);
            default:
                return new Response(
                        Response.METHOD_NOT_ALLOWED,
                        "Wrong method. Try GET/PUT/DELETE".getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * Implements HTTP GET /v0/entity?id=ID -- get data by given key.
     *
     * @param id data key
     * @return HTTP code 200 with data
     *         HTTP code 404
     */
    private Response get(String id) {
        final ByteBuffer key = HttpServiceUtils.wrapIdToBuffer(id);
        final Iterator<Record> range = dao.range(key, DAO.nextKey(key));
        if (range.hasNext()) {
            final Record first = range.next();
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
    private Response put(String id, byte[] body) {
        final ByteBuffer key = HttpServiceUtils.wrapIdToBuffer(id);
        final ByteBuffer value = ByteBuffer.wrap(body);
        dao.upsert(Record.of(key, value));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    /**
     * Implements HTTP DELETE /v0/entity?id=ID -- delete data by given key.
     *
     * @param id data key
     * @return HTTP code 202
     */
    private Response delete(String id) {
        final ByteBuffer key = HttpServiceUtils.wrapIdToBuffer(id);
        dao.upsert(Record.tombstone(key));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }
}
