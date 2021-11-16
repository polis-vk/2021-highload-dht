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
import ru.mail.polis.service.sachuk.ilya.ResponseUtils;
import ru.mail.polis.service.sachuk.ilya.sharding.Node;
import ru.mail.polis.service.sachuk.ilya.sharding.NodeManager;
import ru.mail.polis.service.sachuk.ilya.sharding.NodeRouter;
import ru.mail.polis.service.sachuk.ilya.sharding.VNode;

import java.io.Closeable;
import java.nio.ByteBuffer;
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

    public Coordinator(NodeManager nodeManager, NodeRouter nodeRouter, EntityRequestHandler entityRequestHandler,
                       Node node) {
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

    private List<Response> getResponses(NavigableMap<Integer, VNode> vnodes, String id, Request request,
                                        ReplicationInfo replicationInfo) {
        List<Response> responses = new ArrayList<>();

        int count = 0;
        for (VNode vnode : vnodes.values()) {
            if (count >= replicationInfo.ask) {
                coordinatorExecutor.execute(() -> {
                    if (vnode.getPhysicalNode().port == node.port) {
                        entityRequestHandler.handle(request, id);
                    } else {
                        nodeRouter.routeToNode(vnode, request);
                    }
                });
                continue;
            }

            Response response = chooseHandler(id, request, vnode);

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
                int status = response.getStatus();
                if (status == 200 || status == 404) {
                    Record recordFromResponse = getRecordFromResponse(response, key);
                    records.add(recordFromResponse);
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

    private Response chooseHandler(String id, Request request, VNode vnode) {
        Response response;
        if (vnode.getPhysicalNode().port == node.port) {
            if (logger.isInfoEnabled()) {
                logger.info("HANDLE BY CURRENT NODE: port :" + vnode.getPhysicalNode().port);
            }
            response = entityRequestHandler.handle(request, id);
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
        ThreadUtils.awaitForShutdown(coordinatorExecutor);
    }
}
