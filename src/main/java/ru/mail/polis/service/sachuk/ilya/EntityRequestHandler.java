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
                return addTimeStampHeaderAndTombstone(new Response(Response.NOT_FOUND, Response.EMPTY),
                        Utils.byteBufferToTimestamp(record.getTimestamp()).getTime()
                ); //  если  tombstone то добавляем и таймстем и томбстоун хедер
            }

            logger.info("in get in not tombstone block");
            return addTimeStampHeader(new Response(Response.OK, Utils.bytebufferToBytes(record.getValue())),
                    Utils.byteBufferToTimestamp(record.getTimestamp()).getTime()
            ); // просто добавляем timestamp header
        } else {
            logger.info("in get in block when not found any");
            return new Response(Response.NOT_FOUND, Response.EMPTY); // значит вообще нет, timestamp тоже нет
        }
    }

    private Response delete(String id, Request request) {

        long secs = Long.parseLong(request.getHeader("Timestamp"));

        logger.info("IN DELETE");
        logger.info("Timestamp in delete:" + new Timestamp(secs));

        dao.upsert(Record.tombstone(Utils.stringToBytebuffer(id),
                Utils.timeStampToByteBuffer(secs))
        );

        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private Response put(String id, Request request) {
        long secs = Long.parseLong(request.getHeader("Timestamp"));

        logger.info("IN PUT");

        logger.info("Timestamp in put:" + new Timestamp(secs));

        byte[] body = request.getBody();

        logger.info("WRITE IN PUT :" + body.length + " bytes");
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        ByteBuffer value = ByteBuffer.wrap(body);

        dao.upsert(Record.of(key,
                value,
                Utils.timeStampToByteBuffer(secs))
        );

        return new Response(Response.CREATED, Response.EMPTY);
    }


    private Response addTimeStampHeader(Response response, long secs) {
        response.addHeader("Timestamp" + secs);

        return response;
    }

    private Response addTimeStampHeaderAndTombstone(Response response, long secs) {
        response.addHeader("Timestamp" + secs);
        response.addHeader("Tombstone" + "ddd");

        return response;
    }
}
