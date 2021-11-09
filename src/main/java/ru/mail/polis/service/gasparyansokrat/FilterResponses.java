package ru.mail.polis.service.gasparyansokrat;

import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.lsm.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

final class FilterResponses {

    private FilterResponses() {

    }

    public static Response validResponse(final Request request, final List<Response> responses,
                                   final int requireAck) throws IOException {
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return filterGetResponse(responses, requireAck);
            case Request.METHOD_PUT:
                return filterPutAndDeleteResponse(responses, ServiceImpl.STATUS_CREATED, requireAck);
            case Request.METHOD_DELETE:
                return filterPutAndDeleteResponse(responses, ServiceImpl.STATUS_DELETED, requireAck);
            default:
                throw new IOException("Not allowed method");
        }
    }

    private static Response filterGetResponse(final List<Response> responses, final int requireAck) {
        NavigableMap<Timestamp, Record> filterResponse = new TreeMap<>();
        int ack = 0;
        for (Response response : responses) {
            final int status = response.getStatus();
            if (status == ServiceImpl.STATUS_OK || status == ServiceImpl.STATUS_NOT_FOUND) {
                ack += 1;
                Record record = Record.direct(Record.DUMMY, ByteBuffer.wrap(response.getBody()));
                if (record.isEmpty()) {
                    continue;
                }
                filterResponse.put(record.getTimestamp(), record);
            }
        }
        if (ack < requireAck) {
            return new Response(ClusterService.BAD_REPLICAS, Response.EMPTY);
        }
        if (!filterResponse.isEmpty()) {
            Record sendBuffer = filterResponse.lastEntry().getValue();
            if (!sendBuffer.isTombstone()) {
                return new Response(Response.OK, sendBuffer.getBytesValue());
            }
        }
        return new Response(Response.NOT_FOUND, Response.EMPTY);
    }

    private static Response filterPutAndDeleteResponse(final List<Response> responses, final int status,
                                                final int requireAck) {
        int ack = 0;
        Response response = null;
        for (final Response resp : responses) {
            if (resp.getStatus() == status) {
                response = resp;
                ack += 1;
            }
        }
        if (ack < requireAck) {
            return new Response(ClusterService.BAD_REPLICAS, Response.EMPTY);
        }
        return response;
    }

}
