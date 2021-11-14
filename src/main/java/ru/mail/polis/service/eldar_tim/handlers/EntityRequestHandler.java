package ru.mail.polis.service.eldar_tim.handlers;

import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.Cluster;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.sharding.HashRouter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class EntityRequestHandler extends RequestHandler {

    private final DAO dao;

    public EntityRequestHandler(
            Cluster.Node self, HashRouter<Cluster.Node> router, Cluster.ReplicasHolder replicasHolder,
            DAO dao) {
        super(self, router, replicasHolder);
        this.dao = dao;
    }

    @Nullable
    @Override
    protected String getRouteKey(Request request) {
        return parseId(request);
    }

    @Nonnull
    @Override
    protected ServiceResponse handleRequest(Request request) {
        String id = parseId(request);
        if (id == null) {
            return ServiceResponse.of(new Response(Response.BAD_REQUEST, "Bad id".getBytes(StandardCharsets.UTF_8)));
        }

        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return get(id);
            case Request.METHOD_PUT:
                return put(id, request.getBody());
            case Request.METHOD_DELETE:
                return delete(id);
            default:
                return super.handleRequest(request);
        }
    }

    private ServiceResponse get(@Nonnull String id) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final Iterator<Record> iterator = dao.range(key, DAO.nextKey(key), true);
        if (iterator.hasNext()) {
            Record next = iterator.next();
            if (next.isTombstone()) {
                return ServiceResponse.of(new Response(Response.NOT_FOUND, Response.EMPTY),
                        next.getTimestamp());
            } else {
                return ServiceResponse.of(new Response(Response.OK, extractBytes(next.getValue())),
                        next.getTimestamp());
            }
        } else {
            return ServiceResponse.of(new Response(Response.NOT_FOUND, Response.EMPTY));
        }
    }

    private ServiceResponse put(@Nonnull String id, @Nonnull byte[] body) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        ByteBuffer value = ByteBuffer.wrap(body);
        dao.upsert(Record.of(key, value));
        return ServiceResponse.of(new Response(Response.CREATED, Response.EMPTY));
    }

    private ServiceResponse delete(@Nonnull String id) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        dao.upsert(Record.tombstone(key));
        return ServiceResponse.of(new Response(Response.ACCEPTED, Response.EMPTY));
    }

    @Nullable
    private String parseId(Request request) {
        String id = request.getParameter("id=");
        return (id == null || id.isEmpty()) ? null : id;
    }
}
