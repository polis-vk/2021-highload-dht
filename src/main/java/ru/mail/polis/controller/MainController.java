package ru.mail.polis.controller;

import one.nio.http.Request;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.RecordUtil;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.artem_drozdov.LsmDAO;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.RejectedExecutionException;

public class MainController implements Controller {

    private static final Logger LOGGER = LoggerFactory.getLogger(MainController.class);

    private final DAO dao;

    public MainController(DAO dao) {
        this.dao = dao;
    }

    @SuppressWarnings("unused")
    public Response status(Request request) {
        return new Response(Response.OK, Response.EMPTY);
    }

    @SuppressWarnings("unused")
    public Response entity(String id,
                           Request request) {
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return get(id);
            case Request.METHOD_PUT:
                return put(id, request.getBody());
            case Request.METHOD_DELETE:
                return delete(id);
            default:
                return new Response(Response.METHOD_NOT_ALLOWED,
                        "No such method allowed".getBytes(StandardCharsets.UTF_8));
        }
    }

    private Response get(final String id) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        Iterator<Record> iterator = dao.range(key, DAO.nextKey(key));

        Record record = null;
        if (iterator.hasNext()) {
            record = iterator.next();
        }
        return record == null
            ? new Response(Response.NOT_FOUND, Response.EMPTY)
            : new Response(Response.OK, RecordUtil.extractBytes(record.getValue()));
    }

    private Response put(final String id, final byte[] payload) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        ByteBuffer value = ByteBuffer.wrap(payload);
        try {
            dao.upsert(Record.of(key, value));
        } catch (RejectedExecutionException e) {
            LOGGER.info("Failed to process flush task. Reached limit: {}", LsmDAO.FLUSH_TASKS_LIMIT);
            return new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY);
        }
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response delete(final String id) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        dao.upsert(Record.tombstone(key));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }
}
