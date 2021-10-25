package ru.mail.polis.service.shabinsky_dmitry;

import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import one.nio.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.Executor;

public final class BasicService extends HttpServer implements Service {

    private final Logger LOG = LoggerFactory.getLogger(BasicService.class);
    private final DAO dao;
    private final Executor executor;

    public BasicService(
        final int port,
        final DAO dao,
        Executor executor
    ) throws IOException {
        super(from(port));
        this.dao = dao;
        this.executor = executor;
    }

    private static HttpServerConfig from(final int port) {
        final HttpServerConfig config = new HttpServerConfig();
        final AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        acceptor.reusePort = true;
        config.acceptors = new AcceptorConfig[]{acceptor};
        return config;
    }

    @Path("/v0/status")
    public Response status() {
        return Response.ok("I'm ok");
    }

    @Path("/v0/entity")
    public void entity(
        HttpSession session,
        Request request,
        @Param(value = "id", required = true) final String id
    ) {
        execute(request, session, () -> {
            if (id.isBlank()) {
                return new Response(Response.BAD_REQUEST, toBytes("Bad id"));
            }

            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    return get(id);
                case Request.METHOD_PUT:
                    return put(id, request.getBody());
                case Request.METHOD_DELETE:
                    return delete(id);
                default:
                    return new Response(Response.METHOD_NOT_ALLOWED, toBytes("Wrong method"));
            }
        });
    }

    @Override
    public void handleDefault(
        Request request,
        HttpSession session
    ) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    private Response delete(String id) {
        final ByteBuffer key = ByteBuffer.wrap(toBytes(id));
        dao.upsert(Record.tombstone(key));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private Response put(String id, byte[] body) {
        final ByteBuffer key = ByteBuffer.wrap(toBytes(id));
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
        final ByteBuffer key = ByteBuffer.wrap(toBytes(id));
        final Iterator<Record> range = dao.range(key, DAO.nextKey(key));
        if (range.hasNext()) {
            final Record first = range.next();
            return new Response(Response.OK, extractBytes(first.getValue()));
        }
        return new Response(Response.NOT_FOUND, Response.EMPTY);
    }

    private byte[] toBytes(String text) {
        return Utf8.toBytes(text);
    }

    private void execute(Request request, HttpSession session, Task task) {
        executor.execute(() -> {
            Response call;
            try {
                call = task.call();
            } catch (Exception e) {
                LOG.error("Unexpected exception during method call {}", request.getMethodName(), e);
                sendResponse(session, new Response(Response.INTERNAL_ERROR, toBytes("Something wrong")));
                return;
            }

            sendResponse(session, call);
        });
    }

    private void sendResponse(HttpSession session, Response call) {
        try {
            session.sendResponse(call);
        } catch (Exception e) {
            LOG.error("Can't send response", e);
        }
    }

    @FunctionalInterface
    private static interface Task {
        Response call();
    }
}
