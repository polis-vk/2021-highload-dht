package ru.mail.polis.service.alex;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
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

    private final DAO dao;
    private final ExecutorService executorService;

    public AlexServer(final HttpServerConfig config, final DAO dao) throws IOException {
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
            try {
                httpSession.sendResponse(
                        new Response(Response.BAD_REQUEST, "Empty ID!".getBytes(StandardCharsets.UTF_8))
                );
            } catch (IOException e) {
                LOGGER.error("Can't response to user.", e);
            }
        }

        executorService.execute(() -> {
            try {
                switch (request.getMethod()) {
                    case Request.METHOD_GET:
                        httpSession.sendResponse(
                                getEntity(entityId)
                        );
                        break;
                    case Request.METHOD_PUT:
                        httpSession.sendResponse(
                                putEntity(entityId, request.getBody())
                        );
                        break;
                    case Request.METHOD_DELETE:
                        httpSession.sendResponse(
                                deleteEntity(entityId)
                        );
                        break;
                    default:
                        httpSession.sendResponse(
                                new Response(Response.METHOD_NOT_ALLOWED, "Wrong method!".getBytes(StandardCharsets.UTF_8))
                        );
                        break;
                }
            } catch (IOException e) {
                LOGGER.error("Can't response to user.", e);
            }
        });
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
