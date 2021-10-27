package ru.mail.polis.service.alex;

import one.nio.http.HttpException;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.pool.PoolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.artem_drozdov.Node;
import ru.mail.polis.lsm.artem_drozdov.Topology;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class AlexServer extends HttpServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlexServer.class);
    private static final String PROXY_HEADER = "X-OK-Proxy: true";

    private final DAO dao;
    private final ExecutorService executorService;
    private final Topology topology;

    public AlexServer(final HttpServerConfig config, final DAO dao, final Topology topology) throws IOException {
        super(config);
        this.dao = dao;
        int cores = Runtime.getRuntime().availableProcessors();
        this.executorService = new ThreadPoolExecutor(
                cores,
                cores,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(cores + 1)
        );
        this.topology = topology;
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        final Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }

    @Path("/v0/status")
    public Response getStatus() {
        return Response.ok("Server running...");
    }

    @Path("/v0/entity")
    public void entity(
            final Request request,
            final HttpSession httpSession,
            @Param(value = "id", required = true) final String entityId) {

        if (entityId.isEmpty()) {
            send(httpSession, new Response(
                    Response.BAD_REQUEST,
                    "Empty ID!".getBytes(StandardCharsets.UTF_8)
            ));
        } else {
            executorService.execute(() -> {
                Node node = topology.getNode(entityId);
                if (topology.isMe(node)) {
                    switch (request.getMethod()) {
                        case Request.METHOD_GET:
                            send(httpSession, getEntity(entityId));
                            break;
                        case Request.METHOD_PUT:
                            send(httpSession, putEntity(entityId, request.getBody()));
                            break;
                        case Request.METHOD_DELETE:
                            send(httpSession, deleteEntity(entityId));
                            break;
                        default:
                            send(httpSession, new Response(
                                    Response.METHOD_NOT_ALLOWED,
                                    "Wrong method!".getBytes(StandardCharsets.UTF_8)
                            ));
                            break;
                    }
                } else {
                    proxy(httpSession, request, node);
                }
            });
        }
    }

    @Override
    public synchronized void stop() {
        topology.stop();
        super.stop();
    }

    private void proxy(HttpSession httpSession, Request request, Node node) {
        if (request.getHeader(PROXY_HEADER) != null) {
            LOGGER.error("Proxy from proxy");
            send(
                    httpSession,
                    new Response(Response.INTERNAL_ERROR, Response.EMPTY)
            );
            return;
        }

        Response res;
        try {
            request.addHeader(PROXY_HEADER);
            res = node.getHttpClient().invoke(request);
        } catch (IOException | InterruptedException | PoolException | HttpException e) {
            LOGGER.error("Can't proxy");
            res = new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
        send(httpSession, res);
    }

    private void send(HttpSession httpSession, Response response) {
        try {
            httpSession.sendResponse(response);
        } catch (IOException e) {
            LOGGER.error("Can't response to user.", e);
        }
    }

    private Response getEntity(final String id) {
        final ByteBuffer key = byteBufferFrom(id);
        final Iterator<Record> iteratorRecord = dao.range(key, DAO.nextKey(key));
        if (iteratorRecord.hasNext()) {
            final Record record = iteratorRecord.next();
            return new Response(Response.OK, bytesFrom(record.getValue()));
        } else {
            return new Response(Response.NOT_FOUND, "Not found record!".getBytes(StandardCharsets.UTF_8));
        }
    }

    private Response putEntity(final String id, final byte[] data) {
        final ByteBuffer key = byteBufferFrom(id);
        final ByteBuffer value = ByteBuffer.wrap(data);
        dao.upsert(Record.of(key, value));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response deleteEntity(final String id) {
        final ByteBuffer key = byteBufferFrom(id);
        dao.upsert(Record.tombstone(key));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private static ByteBuffer byteBufferFrom(final String value) {
        return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] bytesFrom(final ByteBuffer byteBuffer) {
        final byte[] result = new byte[byteBuffer.remaining()];
        byteBuffer.get(result);
        return result;
    }
}
