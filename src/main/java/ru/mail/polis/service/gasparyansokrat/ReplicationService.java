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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ReplicationService {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationService.class);
    private final String selfNode;
    private final ServiceDAO serviceDAO;
    private final Map<String, HttpClient> clusterServers;

    private boolean stopSender;
    private final ExecutorService sendDataExecutor;
    private final BlockingQueue<Quartet<Request, String, String, Integer>> senderQueue;

    private static final int DEATH_TIME = 128;
    private static final String DAO_URI_PARAMETER = "/internal/cluster/entity?id=%s";

    ReplicationService(final DAO dao, final String selfNode,
                        final Map<String, HttpClient> clusterServers) {
        this.serviceDAO = new ServiceDAO(dao);
        this.clusterServers = clusterServers;
        this.selfNode = selfNode;
        this.sendDataExecutor = Executors.newSingleThreadExecutor();
        this.senderQueue = new LinkedBlockingQueue<>();
        this.stopSender = false;
        setupExecutor();
    }

    public Set<String> getTopology() {
        return clusterServers.keySet();
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
                    if (LOG.isErrorEnabled()) {
                        LOG.error("Error thread in send data executor: " + e.getMessage());
                    }
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

    public Response handleRequest(final Request request, final int requireAck,
                                  final String id, final List<String> nodes) throws IOException {
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
        Response response = FilterResponses.validResponse(request, responses, requireAck);
        if (!resendNodes.isEmpty()) {
            resendMissedData(request, resendNodes, id);
        }
        return response;
    }

    public Response directRequest(final String id, final Request request) throws IOException {
        return serviceDAO.handleRequest(id, request);
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
