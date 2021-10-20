package ru.mail.polis.service.eldar_tim;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.PathMapper;
import one.nio.http.Request;
import one.nio.http.RequestHandler;
import one.nio.http.Response;
import one.nio.net.Session;
import one.nio.server.AcceptorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;
import ru.mail.polis.service.exceptions.ServerRuntimeException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

/**
 * Service implementation for 2021-highload-dht.
 *
 * @author Eldar Timraleev
 */
public class HttpServerImpl extends HttpServer implements Service {
    private static final Logger LOG = LoggerFactory.getLogger(HttpServerImpl.class);

    private final DAO dao;
    private final ServiceExecutor workers;
    private final PathMapper pathMapper = new PathMapper();

    private final RequestHandler statusHandler;

    public HttpServerImpl(final int port, final DAO dao, ServiceExecutor workers) throws IOException {
        super(buildHttpServerConfig(port));
        this.dao = dao;
        this.workers = workers;

        statusHandler = mapPaths();
    }

    private static HttpServerConfig buildHttpServerConfig(final int port) {
        final HttpServerConfig httpServerConfig = new HttpServerConfig();
        AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.threads = 2;
        acceptorConfig.port = port;
        acceptorConfig.reusePort = true;
        acceptorConfig.deferAccept = true;
        httpServerConfig.acceptors = new AcceptorConfig[]{acceptorConfig};
        return httpServerConfig;
    }

    private RequestHandler mapPaths() {
        RequestHandler statusHandler = (request, session) -> status(session);
        pathMapper.add("/v0/status",
                new int[]{Request.METHOD_GET}, statusHandler);

        pathMapper.add("/v0/entity",
                new int[]{Request.METHOD_GET, Request.METHOD_PUT, Request.METHOD_DELETE}, this::entity);

        return statusHandler;
    }

    @Override
    public synchronized void stop() {
        super.stop();
        workers.awaitAndShutdown();
    }

    @Override
    public void handleRequest(Request request, HttpSession session) {
        RequestHandler requestHandler = pathMapper.find(request.getPath(), request.getMethod());

        if (requestHandler == statusHandler) {
            workers.run(session, this::exceptionHandler, () -> requestHandler.handleRequest(request, session));
        } else if (requestHandler != null) {
            workers.execute(session, this::exceptionHandler, () -> requestHandler.handleRequest(request, session));
        } else {
            handleDefault(request, session);
        }
    }

    @Override
    public void handleDefault(Request request, HttpSession session) {
        try {
            Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
            session.sendResponse(response);
        } catch (IOException e) {
            exceptionHandler(session, new ServerRuntimeException(e));
        }
    }

    private void status(HttpSession session) {
        try {
            Response response = Response.ok(Response.OK);
            session.sendResponse(response);
        } catch (IOException e) {
            exceptionHandler(session, new ServerRuntimeException(e));
        }
    }

    private void entity(Request request, HttpSession session) throws IOException {
        String id = request.getRequiredParameter("id=");
        if (id.isBlank()) {
            Response response = new Response(Response.BAD_REQUEST, "Bad id".getBytes(StandardCharsets.UTF_8));
            session.sendResponse(response);
            return;
        }

        final Response response;
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                response = get(id);
                break;
            case Request.METHOD_PUT:
                response = put(id, request.getBody());
                break;
            case Request.METHOD_DELETE:
                response = delete(id);
                break;
            default:
                response = new Response(Response.METHOD_NOT_ALLOWED);
                break;
        }
        session.sendResponse(response);
    }

    private Response get(String id) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final Iterator<Record> iterator = dao.range(key, DAO.nextKey(key));
        if (iterator.hasNext()) {
            return new Response(Response.OK, extractBytes(iterator.next().getValue()));
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private Response put(String id, byte[] body) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        ByteBuffer value = ByteBuffer.wrap(body);
        dao.upsert(Record.of(key, value));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response delete(String id) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        dao.upsert(Record.tombstone(key));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private static byte[] extractBytes(final ByteBuffer buffer) {
        final byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    private void exceptionHandler(Session session, ServerRuntimeException e) {
        sendError(e.description(), e.httpCode(), (HttpSession) session, e);
    }

    private void sendError(String description, String httpCode, HttpSession session, Exception e) {
        //LOG.warn("Error: {}", description); Занимает место в LOCK-профайле
        try {
            String code = httpCode == null ? Response.INTERNAL_ERROR : httpCode;
            session.sendError(code, e.getMessage());
        } catch (IOException ex) {
            LOG.error("Unable to send error: {}", description, ex);
        }
    }
}
