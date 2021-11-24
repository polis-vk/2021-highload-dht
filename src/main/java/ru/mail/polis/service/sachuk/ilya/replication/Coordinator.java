package ru.mail.polis.service.sachuk.ilya.replication;

import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Utils;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.sachuk.ilya.EntityRequestHandler;
import ru.mail.polis.service.sachuk.ilya.Pair;
import ru.mail.polis.service.sachuk.ilya.ResponseUtils;
import ru.mail.polis.service.sachuk.ilya.sharding.Node;
import ru.mail.polis.service.sachuk.ilya.sharding.NodeManager;
import ru.mail.polis.service.sachuk.ilya.sharding.NodeRouter;
import ru.mail.polis.service.sachuk.ilya.sharding.VNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Coordinator {

    private final Logger logger = LoggerFactory.getLogger(Coordinator.class);

    private final NodeManager nodeManager;
    private final NodeRouter nodeRouter;
    private final EntityRequestHandler entityRequestHandler;
    private final Node node;

    public Coordinator(NodeManager nodeManager, NodeRouter nodeRouter, EntityRequestHandler entityRequestHandler,
                       Node node) {
        this.nodeManager = nodeManager;
        this.nodeRouter = nodeRouter;
        this.entityRequestHandler = entityRequestHandler;
        this.node = node;
    }

    public void handle(ReplicationInfo replicationInfo, String id, Request request, HttpSession session) {

        if (logger.isInfoEnabled()) {
            logger.info("in block IS coordinator");
            logger.info("COORDINATOR NODE IS: " + node.port);
        }

        sendRequest(replicationInfo, id, request, session);
    }

    private void sendRequest(ReplicationInfo replicationInfo, String id, Request request, HttpSession session) {
        ByteBuffer key = Utils.wrap(id);

        Integer hash = null;
        List<Integer> currentPorts = new ArrayList<>();
        List<Response> executedResponses = new CopyOnWriteArrayList<>();
        AtomicInteger counter = new AtomicInteger(0);
        AtomicBoolean alreadyExecuted = new AtomicBoolean();

        AtomicInteger ackCount = new AtomicInteger(replicationInfo.ask);

        for (int i = 0; i < replicationInfo.from; i++) {
            Pair<Integer, VNode> pair = nodeManager.getNearVNodeWithGreaterHash(id, hash, currentPorts);
            hash = pair.key;
            VNode vnode = pair.value;
            currentPorts.add(pair.value.getPhysicalNode().port);

            chooseHandler(id, request, vnode)
                    .thenApplyAsync(response -> {
                        int status = response.getStatus();

                        if (status == 504 || status == 405 || status == 503) {
                            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
                        }

                        executedResponses.add(response);
                        counter.incrementAndGet();
                        ackCount.decrementAndGet();

                        return response;
                    }).whenCompleteAsync((response, throwable) -> {
                        if (alreadyExecuted.get()) {
                            return;
                        }
                        if (throwable != null || counter.get() == replicationInfo.from && ackCount.get() > 0) {
                            sendResponse(session,
                                    alreadyExecuted,
                                    new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY)
                            );
                        }
                        if (counter.get() == replicationInfo.from || ackCount.get() == 0) {
                            Response finalResponse = getFinalResponse(request, key, executedResponses, replicationInfo);
                            sendResponse(session, alreadyExecuted, finalResponse);
                        }
                    });
        }
    }

    private void sendResponse(HttpSession session, AtomicBoolean alreadyExecuted, Response response) {
        try {
            if (!alreadyExecuted.get()) {
                synchronized (Coordinator.class) {
                    if (!alreadyExecuted.get()) {
                        alreadyExecuted.set(true);
                        session.sendResponse(response);
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Response getFinalResponseForGet(Record newestRecord) {
        Response finalResponse;
        if (newestRecord.isTombstone()) {
            finalResponse = new Response(Response.NOT_FOUND, Response.EMPTY);
        } else {
            if (newestRecord.getTimestamp() == 0) {
                finalResponse = new Response(Response.NOT_FOUND, Response.EMPTY);
            } else {
                finalResponse = new Response(Response.OK, Utils.bytebufferToBytes(newestRecord.getValue()));
            }
        }

        return finalResponse;
    }

    private Response getFinalResponse(Request request, ByteBuffer key, List<Response> responses,
                                      ReplicationInfo replicationInfo) {

        Response finalResponse;
        if (responses.size() < replicationInfo.ask) {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }

        if (request.getMethod() == Request.METHOD_GET) {
            Record recordToReturn = null;
            for (Response response : responses) {
                Record recordFromResponse = getRecordFromResponse(response, key);

                recordToReturn = getNewestRecord(recordToReturn, recordFromResponse);
            }

            finalResponse = recordToReturn == null
                    ? new Response(Response.GATEWAY_TIMEOUT)
                    : getFinalResponseForGet(recordToReturn);

        } else if (request.getMethod() == Request.METHOD_DELETE) {
            finalResponse = new Response(Response.ACCEPTED, Response.EMPTY);

        } else if (request.getMethod() == Request.METHOD_PUT) {
            finalResponse = new Response(Response.CREATED, Response.EMPTY);
        } else {
            finalResponse = new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
        }

        return finalResponse;
    }

    private Record getNewestRecord(Record recordToReturn, Record recordFromResponse) {
        if (recordToReturn == null) {
            return recordFromResponse;
        }

        if (recordFromResponse.getTimestamp() > recordToReturn.getTimestamp()) {
            return recordFromResponse;
        }

        if (recordFromResponse.getTimestamp() == recordToReturn.getTimestamp()) {
            if (recordFromResponse.isTombstone()) {
                return recordFromResponse;
            }

            if (!recordFromResponse.isTombstone() && !recordToReturn.isTombstone()) {
                int compare = recordFromResponse.getValue().compareTo(recordToReturn.getValue());

                if (compare > 0) {
                    return recordFromResponse;
                }
            }
        }
        return recordToReturn;
    }

    private Record getRecordFromResponse(Response response, ByteBuffer key) {
        String timestampFromResponse = response.getHeader(ResponseUtils.TIMESTAMP_HEADER);
        String tombstoneHeader = response.getHeader(ResponseUtils.TOMBSTONE_HEADER);

        ByteBuffer value = ByteBuffer.wrap(response.getBody());

        if (timestampFromResponse == null) {
            return Record.of(key, value, 0L);
        } else {
            if (tombstoneHeader == null) {
                return Record.of(key,
                        value,
                        Long.parseLong(timestampFromResponse));
            } else {
                return Record.tombstone(key,
                        Long.parseLong(timestampFromResponse)
                );
            }
        }
    }

    private CompletableFuture<Response> chooseHandler(String id, Request request, VNode vnode) {
        CompletableFuture<Response> response;
        if (vnode.getPhysicalNode().port == node.port) {
            if (logger.isInfoEnabled()) {
                logger.info("HANDLE BY CURRENT NODE: port :" + vnode.getPhysicalNode().port);
            }
            response = CompletableFuture.completedFuture(entityRequestHandler.handle(request, id));
        } else {
            if (logger.isInfoEnabled()) {
                logger.info("HANDLE BY OTHER NODE: port :" + vnode.getPhysicalNode().port);
            }
            response = nodeRouter.routeToNode(vnode, request);
        }

        return response;
    }
}
