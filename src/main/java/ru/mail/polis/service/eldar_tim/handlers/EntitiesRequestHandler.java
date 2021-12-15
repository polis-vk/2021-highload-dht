package ru.mail.polis.service.eldar_tim.handlers;

import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.eldar_tim.StreamingChunk;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class EntitiesRequestHandler extends StreamingRequestHandler {

    private final DAO dao;

    public EntitiesRequestHandler(DAO dao) {
        super();
        this.dao = dao;
    }

    @Nonnull
    @Override
    public StreamingServiceResponse handleRequest(Request request) {
        String start = request.getParameter("start=");
        String end = request.getParameter("end=");

        if (start == null || start.isEmpty() || (end != null && end.isEmpty())) {
            return StreamingServiceResponse.of(new Response(Response.BAD_REQUEST,
                    "Bad parameters".getBytes(StandardCharsets.UTF_8)));
        }

        return get(start, end);
    }

    private StreamingServiceResponse get(@Nonnull String start, @Nullable String end) {
        ByteBuffer keyStart = ByteBuffer.wrap(start.getBytes(StandardCharsets.UTF_8));
        ByteBuffer keyEnd = end == null ? null : ByteBuffer.wrap(end.getBytes(StandardCharsets.UTF_8));

        final Iterator<Record> iterator = dao.range(keyStart, keyEnd == null ? DAO.nextKey(keyStart) : keyEnd);

        return StreamingServiceResponse.of(new Response(Response.OK), () -> {
            Record next = iterator.hasNext() ? iterator.next() : null;
            if (next != null) {
                int contentLength = next.getKeySize() + 1 + next.getValueSize();
                return StreamingChunk.init(contentLength)
                        .append(next.getKey(), next.getKeySize())
                        .append('\n')
                        .append(next.getValue(), next.getValueSize());
            } else {
                return StreamingChunk.empty();
            }
        });
    }
}
