package ru.mail.polis.service.danilaeremenko;

import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

final class MyConfig extends HttpServerConfig {
    MyConfig(int port) {
        AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        acceptorConfig.reusePort = true;
        this.acceptors = new AcceptorConfig[]{acceptorConfig};
    }
}

final class DaoWrapper {
    private final DAO dao;

    DaoWrapper(DAO dao) {
        this.dao = dao;
    }

    Response getEntity(String id) {
        final ByteBuffer id_buffer = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final Iterator<Record> range = this.dao.range(
                id_buffer,
                DAO.nextKey(id_buffer)
        );
        if (range.hasNext()) {
            final Record resRecord = range.next();
            return new Response(
                    Response.ACCEPTED,
                    resRecord.getValue().toString().getBytes(StandardCharsets.UTF_8)
            );
        } else {
            return new Response(Response.NOT_FOUND, "Not Found".getBytes(StandardCharsets.UTF_8));
        }

    }

    Response putEntity(String id, final byte[] body) {
        Record newRecord = Record.of(
                ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8)),
                ByteBuffer.wrap(body)
        );
        this.dao.upsert(newRecord);
        return new Response("Created");
    }

    Response deleteEntity(String id) {
        dao.upsert(
                Record.tombstone(ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8)))
        );
        return new Response(Response.ACCEPTED, "Accepted".getBytes(StandardCharsets.UTF_8));
    }
}

public class BasicService extends HttpServer implements Service {
    private final DaoWrapper daoWrapper;

    public BasicService(int port, DAO dao) throws IOException {
        super(new MyConfig(port));
        this.daoWrapper = new DaoWrapper(dao);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, "Not found".getBytes(StandardCharsets.UTF_8)));
    }

    @Path("/v0/status")
    public Response status() {
        return Response.ok("Okay bro");
    }

    @Path("/v0/entity")
    public Response entity(
            final Request request,
            @Param(value = "id", required = true) final String id
    ) {
        if (id.isBlank()) {
            return new Response(Response.BAD_REQUEST, "Undefined method".getBytes(StandardCharsets.UTF_8));
        }
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return this.daoWrapper.getEntity(id);
            case Request.METHOD_PUT:
                return this.daoWrapper.putEntity(id, request.getBody());
            case Request.METHOD_DELETE:
                return this.daoWrapper.deleteEntity(id);
            default:
                return new Response(Response.BAD_REQUEST, "Undefined method".getBytes(StandardCharsets.UTF_8));
        }

    }
}
