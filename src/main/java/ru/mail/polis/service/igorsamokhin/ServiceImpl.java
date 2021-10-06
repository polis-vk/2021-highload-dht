package ru.mail.polis.service.igorsamokhin;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.RequestMethod;
import one.nio.http.Response;
import one.nio.os.SchedulingPolicy;
import one.nio.server.AcceptorConfig;
import one.nio.util.Utf8;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class ServiceImpl extends HttpServer implements Service {
    private static final String ENDPOINT_V0_STATUS = "/v0/status";
    private static final String ENDPOINT_V0_ENTITY = "/v0/entity";

    public static final String BAD_ID_RESPONSE = "Bad id";

    private final DAO dao;
    private boolean isWorking; //false by default

    public ServiceImpl(int port, DAO dao) throws IOException {
        super(from(port));
        this.dao = dao;
    }

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {
        if (!isWorking) {
            return;
        }

        super.handleRequest(request, session);
    }

    @Override
    public synchronized void start() {
        super.start();
        isWorking = true;
    }

    @Override
    public synchronized void stop() {
        isWorking = false;
        super.stop();
    }

    private static HttpServerConfig from(int port) {
        HttpServerConfig config = new HttpServerConfig();
        AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        acceptor.reusePort = true;

        config.acceptors = new AcceptorConfig[]{acceptor};
        return config;
    }

    private Response badRequest(String message) {
        return new Response(Response.BAD_REQUEST, Utf8.toBytes(message));
    }

    @Path(ENDPOINT_V0_STATUS)
    public Response status() {
        return Response.ok("I'm OK");
    }

    @Override
    public void handleDefault(
            final Request request,
            final HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    @Path(ENDPOINT_V0_ENTITY)
    @RequestMethod(Request.METHOD_GET)
    public Response get(@Param(value = "id", required = true) String id) {
        if (id.isBlank()) {
            return badRequest(BAD_ID_RESPONSE);
        }

        ByteBuffer fromKey = wrapString(id);
        ByteBuffer toKey = DAO.nextKey(fromKey);

        Iterator<Record> range = dao.range(fromKey, toKey);
        if (!range.hasNext()) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }

        byte[] value = extractBytes(range.next().getValue());
        return new Response(Response.OK, value);
    }

    @Path(ENDPOINT_V0_ENTITY)
    @RequestMethod(Request.METHOD_PUT)
    public Response put(@Param(value = "id", required = true) String id,
                        Request request) {
        if (id.isBlank()) {
            return badRequest(BAD_ID_RESPONSE);
        }

        Record record = Record.of(wrapString(id), ByteBuffer.wrap(request.getBody()));
        dao.upsert(record);
        return new Response(Response.CREATED, Response.EMPTY);
    }

    @Path(ENDPOINT_V0_ENTITY)
    @RequestMethod(Request.METHOD_DELETE)
    public Response delete(@Param(value = "id", required = true) String id) {
        if (id.isBlank()) {
            return badRequest(BAD_ID_RESPONSE);
        }

        ByteBuffer key = wrapString(id);
        dao.upsert(Record.tombstone(key));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private ByteBuffer wrapString(String string) {
        return ByteBuffer.wrap(Utf8.toBytes(string));
    }

    private static byte[] extractBytes(final ByteBuffer buffer) {
        final byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }
}
