package ru.mail.polis.service.gasparyansokrat;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.pool.PoolException;
import org.javatuples.Quartet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class ReplicationService {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationService.class);
    private final String selfNode;
    private final ServiceDAO serviceDAO;
    private final Map<String, HttpClient> clusterServers;

    private boolean stopSender = false;
    private final ExecutorService sendDataExecutor;
    private final BlockingQueue<Quartet<Request, String, String, Integer>> senderQueue;

    private static final int DEATH_TIME = 256;
    private static final String DAO_URI_PARAMETER = "/internal/cluster/entity?id=%s";
    public static final String BAD_REPLICAS = "504 Not Enough Replicas";

    ReplicationService(final DAO dao, final String selfNode,
                        final Map<String, HttpClient> clusterServers) {
        this.serviceDAO = new ServiceDAO(dao);
        this.clusterServers = clusterServers;
        this.selfNode = selfNode;
        this.sendDataExecutor = Executors.newSingleThreadExecutor();
        this.senderQueue = new LinkedBlockingDeque<>();
        setupExecutor();
    }

    private void setupExecutor() {
        sendDataExecutor.execute(() -> {
            while (!stopSender) {
                try {
                    if (senderQueue.isEmpty()) {
                        Thread.sleep(5);
                    } else {
                        Quartet<Request, String, String, Integer> data = senderQueue.peek();
                        int counter = data.getValue3() + 1;
                        if (counter != DEATH_TIME) {
                            final Request request = data.getValue0();
                            final String node = data.getValue1();
                            final String id = data.getValue2();
                            Response response = externalRequest(id, request, node);
                            if (response.getStatus() == ServiceImpl.STATUS_BAD_GATEWAY) {
                                senderQueue.put(new Quartet<>(request, node, id, counter));
                            }
                        }
                    }
                } catch(InterruptedException e){
                    LOG.error("Error thread in send data executor: " + e.getMessage());
                }
            }
        });
    }

    public void stop() {
        stopSender = true;
        sendDataExecutor.shutdown();
        try {
            if (!sendDataExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("Error! Await termination stop Replication service...");
            }
        } catch (InterruptedException e) {
            LOG.error("Error! Stop Replication service");
        }
    }

    private Response get(final String node, final String id) {
        try {
            final String uri = String.format(DAO_URI_PARAMETER, id);
            return clusterServers.get(node).get(uri);
        } catch (InterruptedException | PoolException | IOException | HttpException e) {
            LOG.error(e.getMessage());
            return new Response(Response.BAD_GATEWAY, Response.EMPTY);
        }
    }

    private Response put(final String node, final String id, final byte[] data) {
        try {
            final String uri = String.format(DAO_URI_PARAMETER, id);
            return clusterServers.get(node).put(uri, data);
        } catch (InterruptedException | PoolException | IOException | HttpException e) {
            LOG.error(e.getMessage());
            return new Response(Response.BAD_GATEWAY, Response.EMPTY);
        }
    }

    private Response delete(final String node, final String id) {
        try {
            final String uri = String.format(DAO_URI_PARAMETER, id);
            return clusterServers.get(node).delete(uri);
        } catch (InterruptedException | PoolException | IOException | HttpException e) {
            LOG.error(e.getMessage());
            return new Response(Response.BAD_GATEWAY, Response.EMPTY);
        }
    }

    private Response externalRequest(final String id, final Request request, final String node) {
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return get(node, id);
            case Request.METHOD_PUT:
                return put(node, id, request.getBody());
            case Request.METHOD_DELETE:
                return delete(node, id);
            default:
                return new Response(Response.METHOD_NOT_ALLOWED, "Bad request".getBytes(StandardCharsets.UTF_8));
        }
    }

    public Response handleRequest(final Request request, Map<String, String> params,
                                  final List<String> nodes) throws IOException {
        final int requireAck = Integer.parseInt(params.get("ack"));
        final String id = params.get("id");
        List<Response> responses = new ArrayList<>();
        List<String> resendNodes = new ArrayList<>();
        for (String node : nodes) {
            if (node.equals(selfNode)) {
                responses.add(serviceDAO.handleRequest(params, request));
            } else {
                Response resp = externalRequest(id, request, node);
                if (resp.getStatus() == ServiceImpl.STATUS_BAD_GATEWAY) {
                    resendNodes.add(node);
                }
                responses.add(resp);
            }
        }
        Response response = validResponse(request, responses, requireAck);
        resendMissedData(request, resendNodes, id);
        return response;
    }

    private Response validResponse(final Request request, final List<Response> responses,
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

    public Response directRequest(final Map<String, String> params,
                                  final Request request) throws IOException {
        return serviceDAO.handleRequest(params, request);
    }

    private Response filterGetResponse(final List<Response> responses, final int requireAck) {
        Map<Integer, Integer> respmap = new HashMap<>();
        respmap.put(ServiceImpl.STATUS_OK, 0);
        respmap.put(ServiceImpl.STATUS_NOT_FOUND, 0);
        NavigableMap<Timestamp, byte[]> filterResponse = new TreeMap<>();
        int ack = 0;
        for (Response response : responses) {
            final int status = response.getStatus();
            if (status == ServiceImpl.STATUS_OK || status == ServiceImpl.STATUS_NOT_FOUND) {
                ack += 1;
                respmap.put(status, respmap.get(status) + 1);
                if (status == ServiceImpl.STATUS_OK) {
                    Record record = Record.direct(Record.DummyBuffer, ByteBuffer.wrap(response.getBody()));
                    filterResponse.put(record.getTimestamp(), record.getBytesValue());
                }
            }
        }
        if (ack < requireAck) {
            return new Response(BAD_REPLICAS, Response.EMPTY);
        }

        if (respmap.get(ServiceImpl.STATUS_OK) == ack) {
            return new Response(Response.OK, filterResponse.lastEntry().getValue());
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private Response filterPutAndDeleteResponse(final List<Response> responses, final int status,
                                                final int requireAck) {
        int ack = 0;
        Response response = null;
        for (final Response resp : responses) {
            if (resp.getStatus() ==  status) {
                response = resp;
                ack += 1;
            }
        }
        if (ack < requireAck) {
            return new Response(BAD_REPLICAS, Response.EMPTY);
        }
        return response;
    }

    private void resendMissedData(final Request request, final List<String> resendNodes,
                                  final String id) {
        if (request.getMethod() == Request.METHOD_GET) {
            return;
        }
        for (final String node : resendNodes) {
            try {
                senderQueue.put(new Quartet<>(request, node, id, 1));
            } catch (InterruptedException e) {
                LOG.error("Error add new resend request to sender executor");
            }
        }
    }
}
