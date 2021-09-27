package ru.mail.polis.service.acherevichnik;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public final class HttpBasicService extends HttpServer implements Service {
    private final DAO dao;

    public HttpBasicService(final int port, final DAO dao) throws IOException {
        super(from(port));
        this.dao = dao;
    }

    private static HttpServerConfig from(final int port) {
        final HttpServerConfig config = new HttpServerConfig();
        final AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        acceptorConfig.reusePort = true;
        config.acceptors = new AcceptorConfig[]{acceptorConfig};
        return config;
    }

    @Path("/v0/status")
    public Response status() {
        return Response.ok("I'm ok");
    }

    /**
     * Summary.
     * @param request Request
     * @param id id of entity
     * @return Response
     */
    @Path("/v0/entity")
    public Response entity(final Request request,
                           @Param(value = "id", required = true) final String id) {
        if (id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, "id param is empty".getBytes(StandardCharsets.UTF_8));
        }
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return getEntity(id);
            case Request.METHOD_PUT:
                return putEntity(id, request.getBody());
            case Request.METHOD_DELETE:
                return deleteEntity(id);
            default:
                return new Response(Response.METHOD_NOT_ALLOWED, "Wrong method".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        Response response = new Response(Response.BAD_REQUEST, "Not supported path".getBytes(StandardCharsets.UTF_8));
        session.sendResponse(response);
    }

    private Response getEntity(final String id) {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final Iterator<Record> range = dao.range(key, DAO.nextKey(key));
        if (range.hasNext()) {
            final Record first = range.next();
            return Response.ok(extractBytes(first.getValue()));
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private Response putEntity(final String id, final byte[] body) {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final ByteBuffer value = ByteBuffer.wrap(body);
        dao.upsert(Record.of(key, value));
        return new Response(Response.CREATED, id.getBytes(StandardCharsets.UTF_8));
    }

    private Response deleteEntity(final String id) {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        dao.upsert(Record.tombstone(key));
        return new Response(Response.ACCEPTED, "Done!".getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] extractBytes(final ByteBuffer buffer) {
        final byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }
}
