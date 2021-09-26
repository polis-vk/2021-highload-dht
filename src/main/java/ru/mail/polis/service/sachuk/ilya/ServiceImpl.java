package ru.mail.polis.service.sachuk.ilya;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.Utils;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class ServiceImpl extends HttpServer implements Service {

    private final DAO dao;

    public ServiceImpl(int port, DAO dao) throws IOException {
        super(configFrom(port));

        this.dao = dao;
    }

    private static HttpServerConfig configFrom(int port) {
        HttpServerConfig httpServerConfig = new HttpServerConfig();
        AcceptorConfig acceptorConfig = new AcceptorConfig();

        acceptorConfig.port = port;
        acceptorConfig.reusePort = true;

        httpServerConfig.acceptors = new AcceptorConfig[]{acceptorConfig};

        return httpServerConfig;
    }

    @Path(value = "/v0/entity")
    public Response handleRequest(
            Request request,
            @Param(value = "id", required = true) String id
    ) {

        if (id.isBlank()) {
            return new Response(Response.BAD_REQUEST, "Empty id".getBytes(StandardCharsets.UTF_8));
        }

        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return get(id);
            case Request.METHOD_PUT:
                return put(id, request);
            case Request.METHOD_DELETE:
                return delete(id);
            default:
                return new Response(Response.METHOD_NOT_ALLOWED, "Method not allowed".getBytes(StandardCharsets.UTF_8));
        }
    }

    private Response delete(String id) {

        dao.upsert(Record.tombstone(Utils.stringToBytebuffer(id)));

        return new Response(Response.ACCEPTED, "Deleted".getBytes(StandardCharsets.UTF_8));
    }

    private Response put(String id, Request request) {

        byte[] body = request.getBody();
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        ByteBuffer value = ByteBuffer.wrap(body);

        dao.upsert(Record.of(key, value));

        return new Response(Response.CREATED, "Created".getBytes(StandardCharsets.UTF_8));
    }

    private Response get(String id) {

        ByteBuffer fromKey = Utils.stringToBytebuffer(id);

        Iterator<Record> range = dao.range(fromKey, DAO.nextKey(fromKey));

        if (range.hasNext()) {
            Record record = range.next();

            return new Response(Response.OK, Utils.bytebufferToBytes(record.getValue()));
        } else {
            return new Response(Response.NOT_FOUND, "Not found".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Path(value = "/v0/status")
    public Response status() {
        return new Response(Response.OK, "OK".getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, "Bad request".getBytes(StandardCharsets.UTF_8)));
    }
}
