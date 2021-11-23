package ru.mail.polis.service.gasparyansokrat;

import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.Record;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

final class FilterResponses {

    private static final Logger LOG = LoggerFactory.getLogger(FilterResponses.class);
    private static final String errorMessage = "Can't send response to client: {}";

    private static final int DISCARD = -1000;

    private FilterResponses() {

    }

    public static void validResponse(final RequestParameters params,
                                     final HttpSession session,
                                     final List<CompletableFuture<HttpResponse<byte[]>>> responses) {
        switch (params.getHttpMethod()) {
            case Request.METHOD_GET:
                filterGetResponse(responses, session, params.getAck());
                break;
            case Request.METHOD_PUT:
                filterPutAndDeleteResponse(responses, session, HttpURLConnection.HTTP_CREATED, params.getAck());
                break;
            case Request.METHOD_DELETE:
                filterPutAndDeleteResponse(responses, session, HttpURLConnection.HTTP_ACCEPTED, params.getAck());
                break;
            default:
                LOG.error("Not allowed method {}", params.getHttpMethod());
                break;
        }
    }

    private static void filterGetResponse(final List<CompletableFuture<HttpResponse<byte[]>>> responses,
                                          final HttpSession session,
                                          final int requireAck) {
        final AtomicReference<Record> freshEntry = new AtomicReference<>(null);
        final AtomicInteger ack = new AtomicInteger(0);
        final AtomicInteger respSize = new AtomicInteger(0);
        for (final CompletableFuture<HttpResponse<byte[]>> response : responses) {
            response.whenComplete((tmpResponse, ex) -> {
                if (tmpResponse.statusCode() == HttpURLConnection.HTTP_OK
                        || tmpResponse.statusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                    final int localAck = ack.addAndGet(1);
                    if (tmpResponse.body().length != 0) {
                        freshEntry.set(getHttpEntry(tmpResponse, freshEntry.get()));
                    }
                    if (readyGetResponse(localAck, requireAck, freshEntry.get(), session, respSize)) {
                        return;
                    }
                }
                checkBadReplicas(respSize, responses.size(), session);
            }).exceptionally(ex -> {
                checkBadReplicas(respSize, responses.size(), session);
                return null;
            });
        }
    }

    private static void filterPutAndDeleteResponse(final List<CompletableFuture<HttpResponse<byte[]>>> responses,
                                                   final HttpSession session,
                                                   final int status,
                                                   final int requireAck) {
        final AtomicInteger ack = new AtomicInteger(0);
        final AtomicInteger respSize = new AtomicInteger(0);
        for (final CompletableFuture<HttpResponse<byte[]>> response : responses) {
            response.whenComplete((tmpResponse, ex) -> {
                if (tmpResponse.statusCode() == status
                        && checkPutDelResponse(ack, requireAck, session, tmpResponse, respSize)) {
                        return;
                }
                checkBadReplicas(respSize, responses.size(), session);
            }).exceptionally(ex -> {
                checkBadReplicas(respSize, responses.size(), session);
                return null;
            });
        }
    }

    private static Record getHttpEntry(final HttpResponse<byte[]> response,
                                       final Record freshEntry) {
        final Record record = Record.direct(Record.DUMMY, ByteBuffer.wrap(response.body()));
        if (freshEntry == null) {
            return record;
        }
        if (record.getTimestamp().after(freshEntry.getTimestamp())) {
            return record;
        } else {
            return freshEntry;
        }
    }

    public static String code2Str(final int statusCode) {
        switch (statusCode) {
            case HttpURLConnection.HTTP_CREATED:
                return Response.CREATED;
            case HttpURLConnection.HTTP_ACCEPTED:
                return Response.ACCEPTED;
            default:
                return Response.BAD_GATEWAY;
        }
    }

    private static boolean readyGetResponse(final int ack, final int requireAck,
                                            final Record entry, final HttpSession session,
                                            final AtomicInteger respSize) {
        if (ack == requireAck) {
            try {
                if (entry == null || entry.isTombstone()) {
                    session.sendResponse(new Response(Response.NOT_FOUND, Response.EMPTY));
                } else {
                    session.sendResponse(new Response(Response.OK, entry.getBytesValue()));
                }
                respSize.set(DISCARD);
                return true;
            } catch (IOException e) {
                LOG.error(errorMessage, e.getMessage());
            }
        }
        return false;
    }

    private static boolean checkPutDelResponse(final AtomicInteger ack, final int requireAck,
                                            final HttpSession session, final HttpResponse<byte[]> response,
                                            final AtomicInteger respSize) {
        if (ack.addAndGet(1) == requireAck) {
            try {
                session.sendResponse(new Response(code2Str(response.statusCode()), response.body()));
                respSize.set(DISCARD);
                return true;
            } catch (IOException e) {
                LOG.error(errorMessage, e.getMessage());
            }
        }
        return false;
    }

    private static void checkBadReplicas(AtomicInteger size, final int responsesSize, final HttpSession session) {
        if (size.addAndGet(1) == responsesSize) {
            try {
                size.set(DISCARD);
                session.sendResponse(new Response(ClusterService.BAD_REPLICAS, Response.EMPTY));
            } catch (IOException e) {
                LOG.error(errorMessage, e.getMessage());
            }
        }
    }
}
