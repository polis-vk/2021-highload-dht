package ru.mail.polis.service.avightclav;

import one.nio.http.Response;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class LocalService implements Service {
    private final DAO dao;

    public LocalService(DAO dao) {
        this.dao = dao;
    }

    @Override
    public Response status() {
        return Response.ok("I'm ok");
    }

    @Override
    public Response get(@Nonnull final String id) {
        final ByteBuffer keyFrom = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final Iterator<Record> range = this.dao.range(keyFrom, DAO.nextKey(keyFrom));
        if (range.hasNext()) {
            final Record record = range.next();
            return new Response(Response.OK, ServiceServer.extractBytes(record.getValue()));
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    @Override
    public Response put(@Nonnull final String id, @Nonnull byte[] body) {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final ByteBuffer value = ByteBuffer.wrap(body);
        dao.upsert(Record.of(key, value));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    @Override
    public Response delete(@Nonnull final String id) {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        dao.upsert(Record.tombstone(key));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }
}
