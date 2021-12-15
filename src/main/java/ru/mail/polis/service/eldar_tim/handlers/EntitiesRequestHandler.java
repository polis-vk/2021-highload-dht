package ru.mail.polis.service.eldar_tim.handlers;

import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.util.ByteArrayBuilder;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class EntitiesRequestHandler extends StreamingRequestHandler {

    private final DAO dao;

    public EntitiesRequestHandler(DAO dao) {
        this.dao = dao;
    }

    @Nonnull
    @Override
    public StreamingServiceResponse handleRequest(Request request) {
        String start, end;
        try {
            start = request.getRequiredParameter("start=");
            end = request.getParameter("end=");
        } catch (NoSuchElementException e) {
            return StreamingServiceResponse.of(new Response(Response.BAD_REQUEST,
                    e.getMessage().getBytes(StandardCharsets.UTF_8)));
        }

        return get(start, end);
    }

    private StreamingServiceResponse get(@Nonnull String start, @Nullable String end) {
        ByteBuffer keyStart = ByteBuffer.wrap(start.getBytes(StandardCharsets.UTF_8));
        ByteBuffer keyEnd = end == null ? null : ByteBuffer.wrap(start.getBytes(StandardCharsets.UTF_8));

        final Iterator<Record> iterator = dao.range(keyStart, keyEnd == null ? DAO.nextKey(keyStart) : keyEnd);
        if (!iterator.hasNext()) {
            return StreamingServiceResponse.of(new Response(Response.NOT_FOUND, Response.EMPTY));
        }

        Response response = new Response(Response.OK);
        return StreamingServiceResponse.of(response, () -> {
            if (iterator.hasNext()) {
                Record next = iterator.next();

                ByteArrayBuilder arrayBuilder = new ByteArrayBuilder(
                        next.getKeySize() + next.getValueSize() + Character.BYTES);
                arrayBuilder.append(next.getKey(), next.getKeySize());
                arrayBuilder.append('\n');
                arrayBuilder.append(next.getValue(), next.getValueSize());

                return arrayBuilder.buffer();
            } else {
                return null;
            }
        });
    }
}
