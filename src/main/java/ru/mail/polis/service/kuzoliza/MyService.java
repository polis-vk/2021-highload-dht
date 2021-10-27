package ru.mail.polis.service.kuzoliza;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import one.nio.http.HttpClient;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.RequestMethod;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MyService extends HttpServer {

    private final DAO dao;
    private static final Logger LOG = LoggerFactory.getLogger(MyService.class);
    private final ExecutorService executor;
    private final Topology topology;
    private final Map<String, HttpClient> nodeToClient;

    MyService(final HttpServerConfig config, final DAO dao, final int workers, final int queue,
              final Topology topology) throws IOException {
        super(config);
        this.dao = dao;
        this.topology = topology;
        this.nodeToClient = new HashMap<>();

        for (final String node : topology.all()) {
            if (topology.isCurrentNode(node)) {
                continue;
            }
            final HttpClient client = new HttpClient(new ConnectionString(node + "?timeout=1000"));
            if (nodeToClient.put(node, client) != null) {
                throw new IllegalStateException("Duplicate node");
            }
        }

        assert workers > 0;
        assert queue > 0;
        executor = new ThreadPoolExecutor(workers, queue, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queue),
                new ThreadFactoryBuilder()
                        .setNameFormat("worker_%d")
                        .setUncaughtExceptionHandler((t, e) -> LOG.error("Error processing request {}", t, e))
                        .build(),
                new ThreadPoolExecutor.AbortPolicy()
        );
    }

    @Path("/v0/status")
    @RequestMethod(Request.METHOD_GET)
    public void status(final HttpSession session) {
        try {
            executor.execute(() -> {
                try {
                    session.sendResponse(Response.ok("OK"));
                } catch (IOException e) {
                    LOG.error("Can't send OK response", e);
               }
            });
        } catch (Exception e) {
            LOG.error("Execution error", e);
            try {
                session.sendResponse(new Response(Response.SERVICE_UNAVAILABLE));
            } catch (IOException ex) {
                LOG.error("Can't send 500 response", ex);
            }
        }
    }

    /**
     * Put/delete and get data from dao.
     *
     * @param id - key
     * @param request - body of request
     *
     *           code 200 - "success" - successfully got data
     *           code 201 - "created" - successful respond to put request
     *           code 202 - "accepted" - successfully deleted data
     *           code 400 - "bad request" - syntax error (id is empty)
     *           code 404 - "not found" - server can't find the resource
     *           code 405 - "method is not allowed" - method can't be used
     *           code 500 - "internal error" - server is not responding
     */
    @Path("/v0/entity")
    public void entity(final @Param(value = "id", required = true) String id, final Request request,
                       final HttpSession session) throws RejectedExecutionException {
        executor.execute(() -> {
            if (id.isEmpty()) {
                try {
                    session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
                } catch (IOException e) {
                    LOG.error("Can't send bad response", e);
                }

            } else {
                final ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
                final String node = topology.getNode(key);
                if (topology.isCurrentNode(node)) {
                    response(key, request, session);

                } else {
                    try {
                        session.sendResponse(proxy(node, request));
                    } catch (IOException e) {
                        LOG.error("Can't proxy request", e);
                    }
                }
            }
        });
    }

    private void response(final ByteBuffer key, final Request request, final HttpSession session) {
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                getResponse(key, session);
                break;

            case Request.METHOD_PUT:
                putResponse(key, request, session);
                break;

            case Request.METHOD_DELETE:
                deleteResponse(key, session);
                break;

            default:
                try {
                    session.sendResponse(new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY));
                } catch (IOException e) {
                    LOG.error("Can't send 405 response", e);
                }
                break;
        }
    }

    private void getResponse(final ByteBuffer key, final HttpSession session) {
        final Iterator<Record> range = dao.range(key, DAO.nextKey(key));
        if (range.hasNext()) {
            final ByteBuffer value = range.next().getValue();
            sendOKResponse(value, session);
        } else {
            try {
                session.sendResponse(new Response(Response.NOT_FOUND, Response.EMPTY));
            } catch (IOException e) {
                LOG.error("Can't send 404 response", e);
            }
        }
    }

    private void sendOKResponse(final ByteBuffer value, final HttpSession session) {
        try {
            session.sendResponse(Response.ok(toByteArray(value)));
        } catch (IOException e) {
            LOG.error("Can't send OK response", e);
        }
    }

    private void putResponse(final ByteBuffer key, final Request request, final HttpSession session) {
        dao.upsert(Record.of(key, ByteBuffer.wrap(request.getBody())));
        try {
            session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
        } catch (IOException e) {
            LOG.error("Can't send 201 response", e);
        }
    }

    private void deleteResponse(final ByteBuffer key, final HttpSession session) {
        dao.upsert(Record.tombstone(key));
        try {
            session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
        } catch (IOException e) {
            LOG.error("Can't send 202 response", e);
        }
    }

    private static byte[] toByteArray(final ByteBuffer value) {
        if (!value.hasRemaining()) {
            return Response.EMPTY;
        }

        final byte[] response = new byte[value.remaining()];
        value.get(response);
        return response;
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        try {
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
        } catch (IOException e) {
            LOG.error("Can't send bad response", e);
        }
    }

    private Response proxy(final String node, final Request request) throws IOException {
        try {
            request.addHeader("Proxy-for: " + node);
            return nodeToClient.get(node).invoke(request);
        } catch (Exception e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    @Override
    public synchronized void stop() {
        super.stop();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                throw new IllegalStateException("Can't await for termination");
            }
        } catch (InterruptedException e) {
            LOG.error("Can't shutdown executor", e);
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

}
