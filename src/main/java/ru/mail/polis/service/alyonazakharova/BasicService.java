package ru.mail.polis.service.alyonazakharova;

import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class BasicService extends HttpServer implements Service {
    private final DAO dao;

    /**
     * Init server.
     *
     * @param port number or port
     * @param dao data access object
     * @throws IOException
     */
    public BasicService(final int port, final DAO dao) throws IOException {
        super(from(port));
        this.dao = dao;
    }

    /**
     * Generate HttpServerConfig by port number.
     *
     * @param port number of port
     * @return HttpServerConfig which is used in constructor
     */
    private static HttpServerConfig from(final int port) {
        final HttpServerConfig config = new HttpServerConfig();
        final AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        acceptorConfig.reusePort = true;
        config.acceptors = new AcceptorConfig[]{acceptorConfig};
        return config;
    }

    /**
     * Method to check if the server is listening.
     *
     * @return 200 OK
     */
    @Path("/v0/status")
    public Response status() {
        return Response.ok("I'm OK");
    }

    /**
     * Execute get, put or delete operation.
     *
     * @param request method (get/put/delete)
     * @param id record's key
     * @return 405/200/404/201/202 response
     */
    @Path("/v0/entity")
    public Response entity(final Request request, @Param(value = "id", required = true) final String id) {
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
                return new Response(Response.METHOD_NOT_ALLOWED, "Wrong method".getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * Transform ByteBuffer to array of bytes.
     *
     * @param buffer to be transformed
     * @return transformed byte array
     */
    private static byte[] extractBytes(final ByteBuffer buffer) {
        final byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    /**
     * Get a record by key.
     *
     * @param id key
     * @return 200 OK if the record was found and 404 NOT FOUND otherwise
     */
    private Response get(final String id) {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final Iterator<Record> range = dao.range(key, DAO.nextKey(key));
        if (range.hasNext()) {
            final Record first = range.next();
            return new Response(Response.OK, extractBytes(first.getValue()));
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    /**
     * Upsert a record.
     *
     * @param id key
     * @param body value
     * @return 201 CREATED
     */
    private Response put(final String id, final byte[] body) {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final ByteBuffer value = ByteBuffer.wrap(body);
        dao.upsert(Record.of(key, value));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    /**
     * Delete a record by key.
     *
     * @param id key
     * @return 202 ACCEPTED
     */
    private Response delete(final String id) {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        dao.upsert(Record.tombstone(key));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }
}
