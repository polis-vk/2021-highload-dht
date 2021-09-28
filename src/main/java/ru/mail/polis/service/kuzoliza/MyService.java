package ru.mail.polis.service.kuzoliza;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.RequestMethod;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class MyService extends HttpServer {

    private final DAO dao;
    private static final Logger LOG = LoggerFactory.getLogger(MyService.class);

    MyService(final HttpServerConfig config, final DAO dao) throws IOException {
        super(config);
        this.dao = dao;
    }

    @Path("/v0/status")
    @RequestMethod(Request.METHOD_GET)
    public Response status() {
        return Response.ok("OK");
    }

    /**
     * Put/delete and get data from dao.
     *
     * @param id - key
     * @param request - body of request
     *
     * @return - code 200 - "success" - successfully got data
     *           code 201 - "created" - successful respond to put request
     *           code 202 - "accepted" - successfully deleted data
     *           code 400 - "bad request" - syntax error (id is empty)
     *           code 404 - "not found" - server can't find the resource
     *           code 405 - "method is not allowed" - method can't be used
     *           code 500 - "internal error" - server is not responding
     */
    @Path("/v0/entity")
    public Response entity(final @Param(value = "id", required = true) String id, final Request request) {
        if (id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }

        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        try {
            return response(key, request);
        } catch (IOException e) {
            LOG.error("Can't process response {}", id, e);
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    private Response response(final ByteBuffer key, final Request request) throws IOException {
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                try {
                    final Iterator<Record> range = dao.range(key, DAO.nextKey(key));
                    final ByteBuffer value = range.next().getValue();
                    return Response.ok(toByteArray(value));
                } catch (NoSuchElementException e) {
                    LOG.debug("Can't find resource {}", key, e);
                    return new Response(Response.NOT_FOUND, Response.EMPTY);
                }

            case Request.METHOD_PUT:
                dao.upsert(Record.of(key, ByteBuffer.wrap(request.getBody())));
                return new Response(Response.CREATED, Response.EMPTY);

            case Request.METHOD_DELETE:
                dao.upsert(Record.tombstone(key));
                return new Response(Response.ACCEPTED, Response.EMPTY);

            default:
                return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
        }
    }

    private static byte[] toByteArray(final ByteBuffer value) {
        if (!value.hasRemaining()) {
            return Response.EMPTY;
        }

        final byte[] response = new byte[value.remaining()];
        value.get(response);
        return response;
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

}
