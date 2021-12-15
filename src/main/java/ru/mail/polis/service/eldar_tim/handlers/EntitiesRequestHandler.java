package ru.mail.polis.service.eldar_tim.handlers;

import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.eldar_tim.ServiceResponse;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class EntitiesRequestHandler extends RequestHandler {

    private final DAO dao;

    public EntitiesRequestHandler(HandlerContext context, DAO dao) {
        super(context);
        this.dao = dao;
    }

    @Nullable
    @Override
    protected String getRouteKey(Request request) {
        return null;
    }

    @Nonnull
    @Override
    protected ServiceResponse handleRequest(Request request) {
        String start, end;
        try {
            start = request.getRequiredParameter("start=");
            end = request.getParameter("end=");
        } catch (NoSuchElementException e) {
            return ServiceResponse.of(new Response(Response.BAD_REQUEST,
                    e.getMessage().getBytes(StandardCharsets.UTF_8)));
        }

        return get(start, end);
    }

    private ServiceResponse get(@Nonnull String start, @Nullable String end) {
        ByteBuffer keyStart = ByteBuffer.wrap(start.getBytes(StandardCharsets.UTF_8));
        ByteBuffer keyEnd = end == null ? null : ByteBuffer.wrap(start.getBytes(StandardCharsets.UTF_8));

        final Iterator<Record> iterator = dao.range(keyStart, keyEnd == null ? DAO.nextKey(keyStart) : keyEnd);
        if (!iterator.hasNext()) {
            return ServiceResponse.of(new Response(Response.NOT_FOUND, Response.EMPTY));
        }

        while (iterator.hasNext()) {
            Record next = iterator.next();
            // TODO
            return ServiceResponse.of(new Response(Response.OK, extractBytes(next.getValue())));
        }

        return ServiceResponse.of(new Response(Response.NOT_FOUND, Response.EMPTY));
    }
}
