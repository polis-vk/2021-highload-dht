package ru.mail.polis.service.anastasia_tushkanova;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.RequestMethod;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Objects;

public class BasicService extends HttpServer implements Service {
    private final DAO dao;

    public BasicService(final int port, final DAO dao) throws IOException {
        super(from(port));
        this.dao = dao;
    }

    private static HttpServerConfig from(final int port) {
        final AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        acceptorConfig.reusePort = true;
        final HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{acceptorConfig};
        return config;
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    @Path("/v0/status")
    @RequestMethod(Request.METHOD_GET)
    public Response getStatus() {
        return Response.ok("I'm ok".getBytes(StandardCharsets.UTF_8));
    }

    @Path("/v0/entity")
    public Response entity(@Param(value = "id", required = true) final String id,
                           final Request request) {
        if (id.isBlank()) {
            return new Response(Response.BAD_REQUEST, "Blank id".getBytes(StandardCharsets.UTF_8));
        }
        ByteBuffer byteBufferId = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return get(byteBufferId);
            case Request.METHOD_PUT:
                return put(request.getBody(), byteBufferId);
            case Request.METHOD_DELETE:
                return delete(byteBufferId);
            default:
                return new Response(Response.METHOD_NOT_ALLOWED, "Wrong method".getBytes(StandardCharsets.UTF_8));
        }
    }

    private Response get(ByteBuffer id) {
        Iterator<Record> daoIterator = dao.range(id, null);
        if (!daoIterator.hasNext()) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
        Record record = daoIterator.next();
        if (Objects.equals(record.getKey(), id)) {
            return new Response(Response.OK, getBytes(record));
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private Response put(byte[] requestBody, ByteBuffer id) {
        dao.upsert(Record.of(id, ByteBuffer.wrap(requestBody)));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response delete(ByteBuffer id) {
        dao.upsert(Record.tombstone(id));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private byte[] getBytes(Record record) {
        ByteBuffer byteBuffer = record.getValue();
        byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        return bytes;
    }
}
