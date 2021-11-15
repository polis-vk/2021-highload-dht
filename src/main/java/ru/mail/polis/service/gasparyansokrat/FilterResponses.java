package ru.mail.polis.service.gasparyansokrat;

import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.lsm.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

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
        int ack = 0;
        Record freshEntry = null;
        for (final Response response : responses) {
            final int status = response.getStatus();
            if (status == ServiceImpl.STATUS_OK || status == ServiceImpl.STATUS_NOT_FOUND) {
                Record record = Record.direct(Record.DUMMY, ByteBuffer.wrap(response.getBody()));
                ack += 1;
                if (freshEntry == null) {
                    freshEntry = record;
                    continue;
                }
                if (record.getTimestamp().after(freshEntry.getTimestamp())) {
                    freshEntry = record;
                }
            }
        }

        if (ack < requireAck) {
            return new Response(ClusterService.BAD_REPLICAS, Response.EMPTY);
        }
        if (freshEntry != null && !freshEntry.isTombstone()) {
            return new Response(Response.OK, freshEntry.getBytesValue());
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
