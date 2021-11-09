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

    private boolean stopSender;
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
        this.stopSender = false;
        setupExecutor();
    }

    private void setupExecutor() {
        sendDataExecutor.execute(() -> {
            while (!stopSender) {
                try {
                    if (senderQueue.isEmpty()) {
                        Thread.sleep(5);
                    } else {
                        resendData();
                    }
                } catch (InterruptedException e) {
                    LOG.error("Error thread in send data executor: " + e.getMessage());
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    private void resendData() throws InterruptedException {
        Quartet<Request, String, String, Integer> data = senderQueue.peek();
        int counter = data.getValue3() + 1;
        if (counter >= DEATH_TIME) {
            return;
        }
        final Request request = data.getValue0();
        final String node = data.getValue1();
        final String id = data.getValue2();
        Response response = externalRequest(id, request, node);
        if (response.getStatus() == ServiceImpl.STATUS_BAD_GATEWAY) {
            senderQueue.put(new Quartet<>(request, node, id, counter));
        }
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
            Thread.currentThread().interrupt();
        }
    }

    private Response get(final String node, final String uri) {
        try {
            return clusterServers.get(node).get(uri);
        } catch (InterruptedException | PoolException | IOException | HttpException e) {
            LOG.error(e.getMessage());
            return new Response(Response.BAD_GATEWAY, Response.EMPTY);
        }
    }

    private Response put(final String node, final String uri, final byte[] data) {
        try {
            return clusterServers.get(node).put(uri, data);
        } catch (InterruptedException | PoolException | IOException | HttpException e) {
            LOG.error(e.getMessage());
            return new Response(Response.BAD_GATEWAY, Response.EMPTY);
        }
    }

    private Response delete(final String node, final String uri) {
        try {
            return clusterServers.get(node).delete(uri);
        } catch (InterruptedException | PoolException | IOException | HttpException e) {
            LOG.error(e.getMessage());
            return new Response(Response.BAD_GATEWAY, Response.EMPTY);
        }
    }

    private Response externalRequest(final String id, final Request request, final String node) {
        final String uri = String.format(DAO_URI_PARAMETER, id);
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return get(node, uri);
            case Request.METHOD_PUT:
                return put(node, uri, request.getBody());
            case Request.METHOD_DELETE:
                return delete(node, uri);
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
                responses.add(serviceDAO.handleRequest(id, request));
            } else {
                Response resp = externalRequest(id, request, node);
                if (resp.getStatus() == ServiceImpl.STATUS_BAD_GATEWAY) {
                    resendNodes.add(node);
                }
                responses.add(resp);
            }
        }
        Response response = validResponse(request, responses, requireAck);
        if (!resendNodes.isEmpty()) {
            resendMissedData(request, resendNodes, id);
        }
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

    public Response directRequest(final String id, final Request request) throws IOException {
        return serviceDAO.handleRequest(id, request);
    }

    private Response filterGetResponse(final List<Response> responses, final int requireAck) {
        NavigableMap<Timestamp, Record> filterResponse = new TreeMap<>();
        int ack = 0;
        for (Response response : responses) {
            final int status = response.getStatus();
            if (status == ServiceImpl.STATUS_OK || status == ServiceImpl.STATUS_NOT_FOUND) {
                ack += 1;
                Record record = Record.direct(Record.DummyBuffer, ByteBuffer.wrap(response.getBody()));
                if (record.isEmpty()) {
                    continue;
                }
                filterResponse.put(record.getTimestamp(), record);
            }
        }
        if (ack < requireAck) {
            return new Response(BAD_REPLICAS, Response.EMPTY);
        }
        if (!filterResponse.isEmpty()) {
            Record sendBuffer = filterResponse.lastEntry().getValue();
            if (!sendBuffer.isTombstone()) {
                return new Response(Response.OK, sendBuffer.getBytesValue());
            }
        }
        return new Response(Response.NOT_FOUND, Response.EMPTY);
    }

    private Response filterPutAndDeleteResponse(final List<Response> responses, final int status,
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
            return new Response(BAD_REPLICAS, Response.EMPTY);
        }
        return response;
    }

    private void resendMissedData(final Request request, final List<String> resendNodes,
                                  final String id) {
        if (request.getMethod() != Request.METHOD_PUT
                && request.getMethod() != Request.METHOD_DELETE) {
            return;
        }
        for (final String node : resendNodes) {
            try {
                senderQueue.put(new Quartet<>(request, node, id, 1));
            } catch (InterruptedException e) {
                LOG.error("Error add new resend request to sender executor");
                Thread.currentThread().interrupt();
            }
        }
    }
}
