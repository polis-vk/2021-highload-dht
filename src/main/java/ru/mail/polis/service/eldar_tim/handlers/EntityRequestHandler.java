package ru.mail.polis.service.eldar_tim.handlers;

import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.Cluster;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.sharding.HashRouter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class EntityRequestHandler extends RoutingRequestHandler {

    private final DAO dao;

    public EntityRequestHandler(
            Cluster.ReplicasManager replicasManager, Cluster.Node self, HashRouter<Cluster.Node> router,
            DAO dao) {
        super(replicasManager, self, router);
        this.dao = dao;
    }

    @Nullable
    @Override
    protected String getRouteKey(Request request) {
        return request.getParameter("id=");
    }

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {
        String id = request.getRequiredParameter("id=");
        if (id.isEmpty()) {
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

    private Response get(@Nonnull String id) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final Iterator<Record> iterator = dao.range(key, DAO.nextKey(key));
        if (iterator.hasNext()) {
            return new Response(Response.OK, extractBytes(iterator.next().getValue()));
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private Response put(@Nonnull String id, @Nonnull byte[] body) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        ByteBuffer value = ByteBuffer.wrap(body);
        dao.upsert(Record.of(key, value));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response delete(@Nonnull String id) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        dao.upsert(Record.tombstone(key));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }
}
