package ru.mail.polis.service.sachuk.ilya;

import one.nio.http.Request;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Utils;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Iterator;

public class EntityRequestHandler {
    private final Logger logger = LoggerFactory.getLogger(EntityRequestHandler.class);

    private final DAO dao;

    public EntityRequestHandler(DAO dao) {
        this.dao = dao;
    }

    public Response handle(Request request, String id) {
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return get(id);
            case Request.METHOD_PUT:
                return put(id, request);
            case Request.METHOD_DELETE:
                return delete(id, request);
            default:
                return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
        }
    }

    private Response get(String id) {
        logger.info("IN GET");

        ByteBuffer fromKey = Utils.stringToBytebuffer(id);

        Iterator<Record> range = dao.range(fromKey, DAO.nextKey(fromKey));

        if (range.hasNext()) {
            Record record = range.next();

            if (record.isTombstone()) {
                logger.info("in get in tombstone block");
                return ResponseUtils.addTimeStampHeaderAndTombstone(new Response(Response.NOT_FOUND, Response.EMPTY),
                        record.getTimestamp()
                );
            }

            logger.info("in get in not tombstone block");
            return ResponseUtils.addTimeStampHeader(
                    new Response(Response.OK, Utils.bytebufferToBytes(record.getValue())),
                    record.getTimestamp()
            );
        } else {
            logger.info("in get in block when not found any");
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private Response delete(String id, Request request) {

        long secs = Long.parseLong(request.getHeader(ResponseUtils.TIMESTAMP_HEADER));

        if (logger.isInfoEnabled()) {
            logger.info("IN DELETE");
            logger.info("Timestamp in delete:" + new Timestamp(secs));
        }
        dao.upsert(Record.tombstone(Utils.stringToBytebuffer(id),
                secs)
        );

        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private Response put(String id, Request request) {
        long secs = Long.parseLong(request.getHeader(ResponseUtils.TIMESTAMP_HEADER));

        if (logger.isInfoEnabled()) {
            logger.info("IN PUT");
            logger.info("Timestamp in put:" + new Timestamp(secs));
        }

        byte[] body = request.getBody();
        if (logger.isInfoEnabled()) {
            logger.info("WRITE IN PUT :" + body.length + " bytes");
        }
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        ByteBuffer value = ByteBuffer.wrap(body);

        dao.upsert(Record.of(key,
                value,
                secs)
        );

        return new Response(Response.CREATED, Response.EMPTY);
    }
}
