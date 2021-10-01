package ru.mail.polis.service.medalexey;

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

public class ServiceImpl extends HttpServer implements Service {
    private final DAO dao;

    public ServiceImpl(final int port, final DAO dao) throws IOException {
        super(from(port));
        this.dao = dao;
    }

    private static HttpServerConfig from(final int port) {
        final HttpServerConfig config = new HttpServerConfig();
        final AcceptorConfig acceptor = new AcceptorConfig();

        acceptor.port = port;
        acceptor.reusePort = true;
        config.acceptors = new AcceptorConfig[]{acceptor};

        return config;
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    @Path("/v0/status")
    public Response status() {
        return Response.ok("I'm ok");
    }

    /**
     *  Removed, upserted or returned entity depends on request method.
     *
     * @param request - http request
     * @param id entity id
     * @return http response (2xx if ok, 4xx if error)
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
                return new Response(Response.METHOD_NOT_ALLOWED, "Wrong method".getBytes(StandardCharsets.UTF_8));
        }
    }

    private Response delete(String id) {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        dao.upsert(Record.tombstone(key));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private Response put(String id, byte[] body) {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final ByteBuffer value = ByteBuffer.wrap(body);
        dao.upsert(Record.of(key, value));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private static byte[] extractBytes(final ByteBuffer buffer) {
        final byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    private Response get(String id) {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final Iterator<Record> range = dao.range(key, DAO.nextKey(key));
        if (range.hasNext()) {
            final Record first = range.next();
            return new Response(Response.OK, extractBytes(first.getValue()));
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }
}
