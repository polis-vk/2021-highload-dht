package ru.mail.polis.service.sachuk.ilya.replication;

import one.nio.http.Request;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.ThreadUtils;
import ru.mail.polis.Utils;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.sachuk.ilya.EntityRequestHandler;
import ru.mail.polis.service.sachuk.ilya.Pair;
import ru.mail.polis.service.sachuk.ilya.sharding.Node;
import ru.mail.polis.service.sachuk.ilya.sharding.NodeManager;
import ru.mail.polis.service.sachuk.ilya.sharding.NodeRouter;
import ru.mail.polis.service.sachuk.ilya.sharding.VNode;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Coordinator implements Closeable {

    private final Logger logger = LoggerFactory.getLogger(Coordinator.class);

    private final NodeManager nodeManager;
    private final NodeRouter nodeRouter;
    private final EntityRequestHandler entityRequestHandler;
    private final Node node;
    private final ExecutorService coordinatorExecutor = Executors.newCachedThreadPool();

    public Coordinator(NodeManager nodeManager, NodeRouter nodeRouter, EntityRequestHandler entityRequestHandler, Node node) {
        this.nodeManager = nodeManager;
        this.nodeRouter = nodeRouter;
        this.entityRequestHandler = entityRequestHandler;
        this.node = node;
    }

    public Response handle(ReplicationInfo replicationInfo, String id, Request request) {
        ByteBuffer key = Utils.wrap(id);

        if (logger.isInfoEnabled()) {

            logger.info("in block IS coordinator");
            logger.info("COORDINATOR NODE IS: " + node.port);
        }
        NavigableMap<Integer, VNode> vnodes = getNodes(replicationInfo, id);

        List<Response> responses = getResponses(vnodes, id, request, replicationInfo);

        Response finalResponse = getFinalResponse(request, key, responses, replicationInfo);

        if (logger.isInfoEnabled()) {
            logger.info("FINAL RESPONSE:" + finalResponse.getStatus());
        }

        return finalResponse;
    }

    //FIXME
    private List<Response> getResponses(NavigableMap<Integer, VNode> vnodes, String id, Request request,
                                        ReplicationInfo replicationInfo) {
        List<Response> responses = new ArrayList<>();

        int count = 0;
        for (VNode vNode : vnodes.values()) {
            if (count >= replicationInfo.ask) {
                coordinatorExecutor.execute(() -> {
                    if (vNode.getPhysicalNode().port == node.port) {
                        entityRequestHandler.handle(request, id);
                    } else {
                        nodeRouter.routeToNode(vNode, request);
                    }
                });

                continue;
            }

            Response response = chooseHandler(id, request, vNode);


            if (response.getStatus() == 504 || response.getStatus() == 405) {
                continue;
            }
            count++;
            responses.add(response);
        }

        return responses;
    }

    private NavigableMap<Integer, VNode> getNodes(ReplicationInfo replicationInfo, String id) {
        NavigableMap<Integer, VNode> vnodes = new TreeMap<>();
        List<Integer> currentPorts = new ArrayList<>();
        Integer hash = null;

        for (int i = 0; i < replicationInfo.from; i++) {
            if (logger.isInfoEnabled()) {
                logger.info("In cycle i is : " + i);
                logger.info(String.valueOf(currentPorts));
            }
            Pair<Integer, VNode> pair = nodeManager.getNearVNodeWithGreaterHash(id, hash, currentPorts);
            vnodes.put(pair.key, pair.value);
            hash = pair.key;
            currentPorts.add(pair.value.getPhysicalNode().port);
        }

        if (logger.isInfoEnabled()) {
            logger.info("vnodeList size is: " + vnodes.size() + " and from is: " + replicationInfo.from);
            logger.info("nodes to handle: " + currentPorts);
        }
        return vnodes;
    }

    private Response getFinalResponseForGet(Record newestRecord) {
        Response finalResponse;
        if (newestRecord.isTombstone()) {
            finalResponse = new Response(Response.NOT_FOUND, Response.EMPTY);
        } else {
            if (Utils.byteBufferToTimestamp(newestRecord.getTimestamp()).getTime() == 0) {
                finalResponse = new Response(Response.NOT_FOUND, Response.EMPTY);
            } else {
                finalResponse = new Response(Response.OK, Utils.bytebufferToBytes(newestRecord.getValue()));
            }
        }
        return finalResponse;
    }


    //FIXME
    private Response getFinalResponse(Request request, ByteBuffer key, List<Response> responses,
                                      ReplicationInfo replicationInfo) {

        List<Record> records = new ArrayList<>();

        Response finalResponse;
        if (responses.size() < replicationInfo.ask) {
            if (logger.isInfoEnabled()) {
                logger.info("RESPONSES SIZE IS LESS THEN ASK");
            }
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }

        if (request.getMethod() == Request.METHOD_GET) {
            for (Response response : responses) {
                int status = response.getStatus();

                if (status == 200 || status == 404) {

                    String timestampFromResponse = response.getHeader("Timestamp");
                    String tombstoneHeader = response.getHeader("Tombstone");

                    ByteBuffer value = ByteBuffer.wrap(response.getBody());
                    if (timestampFromResponse == null) {
                        records.add(Record.of(key, value, Utils.timeStampToByteBuffer(0L)));
                    } else {
                        if (tombstoneHeader == null) {
                            records.add(Record.of(key, value, Utils.timeStampToByteBuffer(Long.parseLong(timestampFromResponse))));
                        } else {
                            records.add(Record.tombstone(key, Utils.timeStampToByteBuffer(Long.parseLong(timestampFromResponse))));
                        }
                    }
                }
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

    private Record getNewestRecord(List<Record> records) {
        records.sort((o1, o2) -> {
            Timestamp timestamp1 = Utils.byteBufferToTimestamp(o1.getTimestamp());
            Timestamp timestamp2 = Utils.byteBufferToTimestamp(o2.getTimestamp());

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

    private Response chooseHandler(String id, Request request, VNode vNode) {
        Response response;
        if (vNode.getPhysicalNode().port == node.port) {
            if (logger.isInfoEnabled()) {
                logger.info("HANDLE BY CURRENT NODE: port :" + vNode.getPhysicalNode().port);
            }
            response = entityRequestHandler.handle(request, id);
        } else {
            if (logger.isInfoEnabled()) {
                logger.info("HANDLE BY OTHER NODE: port :" + vNode.getPhysicalNode().port);
            }
            response = nodeRouter.routeToNode(vNode, request);
        }

        return response;
    }

    @Override
    public void close() {
        ThreadUtils.awaitForShutdown(coordinatorExecutor);
    }
}
