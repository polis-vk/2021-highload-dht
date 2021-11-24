package ru.mail.polis.service.sachuk.ilya.replication;

import one.nio.http.Request;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Utils;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.sachuk.ilya.ConfiguredPoolExecutor;
import ru.mail.polis.service.sachuk.ilya.EntityRequestHandler;
import ru.mail.polis.service.sachuk.ilya.ExecutorConfig;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Coordinator implements Closeable {

    private final Logger logger = LoggerFactory.getLogger(Coordinator.class);

    private final NodeManager nodeManager;
    private final NodeRouter nodeRouter;
    private final EntityRequestHandler entityRequestHandler;
    private final Node node;
    private final ConfiguredPoolExecutor coordinatorExecutor =
            new ConfiguredPoolExecutor(new ExecutorConfig(8, 2000));

    public Coordinator(NodeManager nodeManager, NodeRouter nodeRouter, EntityRequestHandler entityRequestHandler,
                       Node node) {
        this.nodeManager = nodeManager;
        this.nodeRouter = nodeRouter;
        this.entityRequestHandler = entityRequestHandler;
        this.node = node;
    }

    public Response handle(ReplicationInfo replicationInfo, String id, Request request) {
        if (coordinatorExecutor.isQueueFull()) {
            return new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY);
        }

        ByteBuffer key = Utils.wrap(id);

        if (logger.isInfoEnabled()) {
            logger.info("in block IS coordinator");
            logger.info("COORDINATOR NODE IS: " + node.port);
        }

        List<Response> responses = getResponses(replicationInfo, id, request);

        Response finalResponse = getFinalResponse(request, key, responses, replicationInfo);

        if (logger.isInfoEnabled()) {
            logger.info("FINAL RESPONSE:" + finalResponse.getStatus());
        }

        return finalResponse;
    }

    private List<Response> getResponses(ReplicationInfo replicationInfo, String id, Request request) {
        List<Response> responses = new ArrayList<>();
        List<Future<Response>> futures = getFutures(replicationInfo, id, request);

        int counter = 0;
        for (Future<Response> future : futures) {
            try {
                if (counter >= replicationInfo.ask) {
                    break;
                }

                Response response = future.get();

                int status = response.getStatus();
                if (status == 504 || status == 405 || status == 503) {
                    continue;
                }

                counter++;
                responses.add(response);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            } catch (ExecutionException e) {
                throw new IllegalStateException(e);
            }
        }

        return responses;
    }

    private List<Future<Response>> getFutures(ReplicationInfo replicationInfo, String id, Request request) {
        List<Future<Response>> futures = new ArrayList<>();
        Integer hash = null;
        List<Integer> currentPorts = new ArrayList<>();

        for (int i = 0; i < replicationInfo.from; i++) {
            Pair<Integer, VNode> pair = nodeManager.getNearVNodeWithGreaterHash(id, hash, currentPorts);
            hash = pair.key;
            VNode vnode = pair.value;
            currentPorts.add(pair.value.getPhysicalNode().port);

            futures.add(
                    coordinatorExecutor.submit(() -> chooseHandler(id, request, vnode))
            );
        }

        return futures;
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
        coordinatorExecutor.close();
    }
}
