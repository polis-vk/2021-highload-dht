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

    @Override
    protected DTO handleRequest(Request request) {
        String id = parseId(request);
        if (id == null) {
            return DTO.answer(Response.BAD_REQUEST, "Bad id".getBytes(StandardCharsets.UTF_8));
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

    @Nonnull
    private DTO get(@Nonnull String id) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final Iterator<Record> iterator = dao.range(key, DAO.nextKey(key));
        if (iterator.hasNext()) {
            Record next = iterator.next();
            return DTO.answer(Response.OK, extractBytes(next.getValue()), next.getTimestamp());
        } else {
            return DTO.answer(Response.NOT_FOUND);
        }
    }

    private DTO put(@Nonnull String id, @Nonnull byte[] body) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        ByteBuffer value = ByteBuffer.wrap(body);
        dao.upsert(Record.of(key, value));
        return DTO.answer(Response.CREATED);
    }

    private DTO delete(@Nonnull String id) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        dao.upsert(Record.tombstone(key));
        return DTO.answer(Response.ACCEPTED);
    }

    @Nullable
    private String parseId(Request request)  {
        String id = request.getRequiredParameter("id=");
        return id.isEmpty() ? null : id;
    }

//    @Override
//    public void handleRequest(Request request, HttpSession session) throws IOException {
//        String id = request.getRequiredParameter("id=");
//        if (id.isEmpty()) {
//            Response response = new Response(Response.BAD_REQUEST, "Bad id".getBytes(StandardCharsets.UTF_8));
//            session.sendResponse(response);
//            return;
//        }
//
//        final Response response;
//        switch (request.getMethod()) {
//            case Request.METHOD_GET:
//                response = get(id);
//                break;
//            case Request.METHOD_PUT:
//                response = put(id, request.getBody());
//                break;
//            case Request.METHOD_DELETE:
//                response = delete(id);
//                break;
//            default:
//                response = new Response(Response.METHOD_NOT_ALLOWED);
//                break;
//        }
//        session.sendResponse(response);
//    }

//    private Response get(@Nonnull String id) {
//        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
//        final Iterator<Record> iterator = dao.range(key, DAO.nextKey(key));
//        if (iterator.hasNext()) {
//            return new Response(Response.OK, extractBytes(iterator.next().getValue()));
//        } else {
//            return new Response(Response.NOT_FOUND, Response.EMPTY);
//        }
//    }
//
//    private Response put(@Nonnull String id, @Nonnull byte[] body) {
//        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
//        ByteBuffer value = ByteBuffer.wrap(body);
//        dao.upsert(Record.of(key, value));
//        return new Response(Response.CREATED, Response.EMPTY);
//    }
//
//    private Response delete(@Nonnull String id) {
//        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
//        dao.upsert(Record.tombstone(key));
//        return new Response(Response.ACCEPTED, Response.EMPTY);
//    }
}
