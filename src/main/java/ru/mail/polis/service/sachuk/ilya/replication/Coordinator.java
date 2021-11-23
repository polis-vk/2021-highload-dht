package ru.mail.polis.service.sachuk.ilya.replication;

import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.Session;
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

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Coordinator implements Closeable {

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

        ByteBuffer key = Utils.wrap(id);

        if (logger.isInfoEnabled()) {
            logger.info("in block IS coordinator");
            logger.info("COORDINATOR NODE IS: " + node.port);
        }

        sendRequest(replicationInfo, id, request, key, session);

        if (logger.isInfoEnabled()) {
//            logger.info("FINAL RESPONSE:" + finalResponse.getStatus());
        }

    }

//    private List<Response> getResponses(ReplicationInfo replicationInfo, String id, Request request, ByteBuffer key) {
//        List<Response> responses = new ArrayList<>();
//
//        int counter = 0;
//        for (CompletableFuture<Response> future : futures) {
//            try {
//                if (counter >= replicationInfo.ask) {
//                    break;
//                }
//
//                Response response = future.get();
//
//                counter++;
//                responses.add(response);
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//                throw new IllegalStateException(e);
//            } catch (ExecutionException e) {
//                throw new IllegalStateException(e);
//            }
//        }
//
//        return responses;
//    }

    private void sendRequest(ReplicationInfo replicationInfo, String id, Request request, ByteBuffer key, HttpSession session) {
        Integer hash = null;
        List<Integer> currentPorts = new ArrayList<>();
        List<Response> executedResponses = new ArrayList<>();
        AtomicInteger counter = new AtomicInteger(0);
        AtomicBoolean sendToExecute = new AtomicBoolean(false);

        AtomicInteger ackCount = new AtomicInteger(replicationInfo.ask);

        for (int i = 0; i < replicationInfo.from; i++) {
            Pair<Integer, VNode> pair = nodeManager.getNearVNodeWithGreaterHash(id, hash, currentPorts);
            hash = pair.key;
            VNode vnode = pair.value;
            currentPorts.add(pair.value.getPhysicalNode().port);
            logger.info("counter for : " + i);

            int cc = i;

            chooseHandler(id, request, vnode)
                    .thenAccept(response -> {
                        if (sendToExecute.get()) {
                            return;
                        }

                        logger.info("in thenAccept and counter is " + cc);
                        int status = response.getStatus();
                        logger.info("status " + status);
                        if (status == 504 || status == 405 || status == 503) {
                            logger.info("from is " + replicationInfo.from + " and curr ack = " + ackCount.get());
                            if (replicationInfo.from == cc + 1 || ackCount.get() > 0) {
                                try {
                                    session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                            logger.info("in return and counter is " + cc);

                            return;
                        }

                        executedResponses.add(response);
                        counter.incrementAndGet();
                        ackCount.decrementAndGet();

                        if (counter.get() >= replicationInfo.ask) {
                            sendToExecute.set(true);
                            Response finalResponse = getFinalResponse(request, key, executedResponses, replicationInfo);
                            try {
                                session.sendResponse(finalResponse);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }

                    });

            logger.info("after future and counter is " + cc);
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

        List<Record> records = new ArrayList<>();

        Response finalResponse;
        if (responses.size() < replicationInfo.ask) {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }

        if (request.getMethod() == Request.METHOD_GET) {
            for (Response response : responses) {
                Record recordFromResponse = getRecordFromResponse(response, key);
                records.add(recordFromResponse);
            }

            Record newestRecord = getNewestRecord(records);
            finalResponse = getFinalResponseForGet(newestRecord);

        } else if (request.getMethod() == Request.METHOD_DELETE) {
            finalResponse = new Response(Response.ACCEPTED, Response.EMPTY);

        } else if (request.getMethod() == Request.METHOD_PUT) {
            finalResponse = new Response(Response.CREATED, Response.EMPTY);
        } else {
            finalResponse = new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
        }

        return finalResponse;
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

    private Record getNewestRecord(List<Record> records) {
        records.sort((o1, o2) -> {
            Long timestamp1 = o1.getTimestamp();
            Long timestamp2 = o2.getTimestamp();

            int compare = timestamp2.compareTo(timestamp1);

            if (compare != 0) {
                return compare;
            }

            if (o1.getValue() == null && o2.getValue() == null) {
                return 1;
            }

            if (o1.getValue() == null) {
                return -1;
            }
            if (o2.getValue() == null) {
                return 1;
            }

            if (o1.getValue().remaining() == 0) {
                return -1;
            }

            if (o2.getValue().remaining() == 0) {
                return 1;
            }

            return o2.getValue().compareTo(o1.getValue());
        });

        return records.get(0);
    }

    private CompletableFuture<Response> chooseHandler(String id, Request request, VNode vnode) {
        CompletableFuture<Response> response;
        if (vnode.getPhysicalNode().port == node.port) {
            if (logger.isInfoEnabled()) {
                logger.info("HANDLE BY CURRENT NODE: port :" + vnode.getPhysicalNode().port);
            }
//            response = CompletableFuture.supplyAsync(() -> entityRequestHandler.handle(request, id));
            response = CompletableFuture.completedFuture(entityRequestHandler.handle(request, id));
        } else {
            if (logger.isInfoEnabled()) {
                logger.info("HANDLE BY OTHER NODE: port :" + vnode.getPhysicalNode().port);
            }
            response = nodeRouter.routeToNode(vnode, request);
        }

        return response;
    }

    @Override
    public void close() {
//        ThreadUtils.awaitForShutdown(coordinatorExecutor);
    }
}
