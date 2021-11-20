package ru.mail.polis.service.gasparyansokrat;

import one.nio.http.Request;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.Record;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

final class FilterResponses {

    private static final Logger LOG = LoggerFactory.getLogger(FilterResponses.class);

    private FilterResponses() {

    }

    public static Response validResponse(final Request request,
                                         final List<CompletableFuture<Response>> responses,
                                         final int requireAck) throws IOException {
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return filterGetResponse(responses, requireAck);
            case Request.METHOD_PUT:
                return filterPutAndDeleteResponse(responses, HttpURLConnection.HTTP_CREATED, requireAck);
            case Request.METHOD_DELETE:
                return filterPutAndDeleteResponse(responses, HttpURLConnection.HTTP_ACCEPTED, requireAck);
            default:
                throw new IOException("Not allowed method");
        }
    }

    private static Response filterGetResponse(final List<CompletableFuture<Response>> responses,
                                              final int requireAck) {
        int ack = 0;
        Record freshEntry = null;
        for (final CompletableFuture<Response> response : responses) {
            final Response localResponse = takeHttpResponse(response);
            if (localResponse == null) {
                continue;
            }
            if (localResponse.getStatus() == HttpURLConnection.HTTP_OK ||
                    localResponse.getStatus() == HttpURLConnection.HTTP_NOT_FOUND) {
                ack += 1;
                if (!ResponseIsEmpty(localResponse)) {
                    freshEntry = getHttpEntry(localResponse, freshEntry);
                }
            }
            Response readyResponse = checkAckResponse(ack, requireAck, freshEntry);
            if (readyResponse != null) {
                return readyResponse;
            }
        }

        return new Response(ClusterService.BAD_REPLICAS, Response.EMPTY);
    }

    private static Response filterPutAndDeleteResponse(final List<CompletableFuture<Response>> responses,
                                                       final int status,
                                                       final int requireAck) {
        int ack = 0;
        Response localResponse = null;
        for (final CompletableFuture<Response> response : responses) {
            final Response tmpResponse = takeHttpResponse(response);
            if (tmpResponse == null) {
                continue;
            }
            if (tmpResponse.getStatus() == status) {
                localResponse = tmpResponse;
                ack += 1;
            }
            if (ack >= requireAck) {
                return localResponse;
            }
        }
        return new Response(ClusterService.BAD_REPLICAS, Response.EMPTY);
    }

    private static boolean ResponseIsEmpty(final Response response) {
        return response.getBody().length == 0;
    }

    public static String code2Str(final int statusCode) {
        switch (statusCode) {
            case HttpURLConnection.HTTP_OK:
                return Response.OK;
            case HttpURLConnection.HTTP_NOT_FOUND:
                return Response.NOT_FOUND;
            case HttpURLConnection.HTTP_CREATED:
                return Response.CREATED;
            case HttpURLConnection.HTTP_ACCEPTED:
                return Response.ACCEPTED;
            case HttpURLConnection.HTTP_BAD_METHOD:
                return Response.METHOD_NOT_ALLOWED;
            default:
                return Response.BAD_GATEWAY;
        }
    }
    
    private static Response takeHttpResponse(final CompletableFuture<Response> response) {
        try {
            return response.get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Error response from node {}", e.getMessage());
            return null;
        }
    }

    private static Record getHttpEntry(final Response response, final Record freshEntry) {
        Record record = Record.direct(Record.DUMMY, ByteBuffer.wrap(response.getBody()));
        if (freshEntry == null) {
            return record;
        }
        if (record.getTimestamp().after(freshEntry.getTimestamp())) {
            return record;
        } else {
            return freshEntry;
        }
    }

    private static Response checkAckResponse(final int ack, final int requireAck, final Record entry) {
        if (ack >= requireAck) {
            if (entry != null && !entry.isTombstone()) {
                return new Response(Response.OK, entry.getBytesValue());
            } else {
                return new Response(Response.NOT_FOUND, Response.EMPTY);
            }
        } else {
            return null;
        }
    }

}
