package ru.mail.polis.service.avightclav;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Server;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BasicService extends HttpServer implements Service {
    private final DAO dao;
    private final ExecutorService executor = Executors.newWorkStealingPool();

    private static final Logger LOG = LoggerFactory.getLogger(Server.class);

    public BasicService(final int port, final DAO dao) throws IOException {
        super(from(port));
        this.dao = dao;
    }

    protected static HttpServerConfig from(final int port) {
        final HttpServerConfig config = new HttpServerConfig();
        final AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        acceptor.reusePort = true;
        config.acceptors = new AcceptorConfig[]{acceptor};
        return config;
    }

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {
        executor.execute(() -> {
            try {
                String path = request.getPath();
                switch (path) {
                    case "/v0/status":
                        if (request.getMethod() == Request.METHOD_GET) {
                            executor.execute(() -> {
                                try {
                                    session.sendResponse(this.status());
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            });
                        }
                        break;
                    case "/v0/entity":
                        final String id = request.getParameter("id", "=");
                        if (id.equals("=")) {
                            session.sendResponse(new Response(Response.BAD_REQUEST, "12".getBytes()));
                            return;
                        }
                        switch (request.getMethod()) {
                            case Request.METHOD_GET:
                                session.sendResponse(this.get(id));
                                return;
                            case Request.METHOD_PUT:
                                session.sendResponse(this.put(id, request.getBody()));
                                return;
                            case Request.METHOD_DELETE:
                                session.sendResponse(this.delete(id));
                                return;
                            default:
                                session.sendResponse(new Response(
                                        Response.METHOD_NOT_ALLOWED,
                                        "Wrong method".getBytes(StandardCharsets.UTF_8))
                                );
                                return;
                        }
                    default:
                        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
                }
            } catch (IOException e) {
                LOG.error("Request handling IO exception", e);
            }
        });
    }

    public Response status() {
        return Response.ok("I'm ok");
    }

    public Response entity(
            final Request request,
            @Param(value = "id", required = true) final String id) {
        if (id.isBlank()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return this.get(id);
            case Request.METHOD_PUT:
                return this.put(id, request.getBody());
            case Request.METHOD_DELETE:
                return this.delete(id);
            default:
                return new Response(Response.METHOD_NOT_ALLOWED, "Wrong method".getBytes(StandardCharsets.UTF_8));
        }
    }

    protected static byte[] extractBytes(final ByteBuffer byteBuffer) {
        byte[] buffer = new byte[byteBuffer.remaining()];
        byteBuffer.get(buffer);
        return buffer;
    }

    private Response get(final String id) {
        final ByteBuffer keyFrom = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final Iterator<Record> range = this.dao.range(keyFrom, DAO.nextKey(keyFrom));
        if (range.hasNext()) {
            final Record record = range.next();
            return new Response(Response.OK, BasicService.extractBytes(record.getValue()));
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private Response put(final String id, byte[] body) {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final ByteBuffer value = ByteBuffer.wrap(body);
        dao.upsert(Record.of(key, value));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response delete(final String id) {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        dao.upsert(Record.tombstone(key));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

}
