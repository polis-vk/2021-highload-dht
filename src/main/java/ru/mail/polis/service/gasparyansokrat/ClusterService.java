package ru.mail.polis.service.gasparyansokrat;

import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ClusterService {

    private final ReplicationService replicationService;
    private final ConsistentHash clusterNodes;
    private final int topologySize;
    private final int quorumCluster;

    private final ExecutorService futurePool;

    public static final String BAD_REPLICAS = "504 Not Enough Replicas";

    ClusterService(final DAO dao, final Set<String> topology, final ServiceConfig servConf) {
        this.clusterNodes = new ConsistentHashImpl(topology);
        this.topologySize = topology.size();
        this.futurePool = Executors.newFixedThreadPool(servConf.poolSize / 2);
        Map<String, HttpClient> clusterServers = buildTopology(servConf, topology);
        this.replicationService = new ReplicationService(dao, servConf.fullAddress, clusterServers);
        this.quorumCluster = topologySize / 2 + 1;
    }

    private Map<String, HttpClient> buildTopology(final ServiceConfig servConf, final Set<String> topology) {
        Map<String, HttpClient> clusterServers = new HashMap<>();
        for (final String node : topology) {
            if (!node.equals(servConf.fullAddress)) {
                clusterServers.put(node, HttpClient.newBuilder()
                                                    .executor(futurePool)
                                                    .build());
            }
        }
        return clusterServers;
    }

    public void handleRequest(final HttpSession session,
                              final RequestParameters params) throws IOException {
        if (params.getStartKey().isEmpty() || !validParameter(params.getAck(), params.getFrom())) {
            session.sendResponse(badRequest());
            return;
        }
        if (params.getHttpMethod() == Request.METHOD_PUT) {
            addTimeStamp(params);
        }

        final List<String> nodes = clusterNodes.getNodes(params.getStartKey(), params.getFrom());
        replicationService.handleRequest(params, session, nodes);
    }

    public void handleRangeRequest(final HttpSession session,
                                   final RequestParameters params) throws IOException {
        final String startKey = params.getStartKey();
        if (startKey.isEmpty()) {
            session.sendResponse(badRequest());
            return;
        }
        final String endKey = params.getEndKey();
        DataTransferChunk rangeData = replicationService.getRangeRequest(startKey, endKey);
        StreamHttpSession streamHttpSession = (StreamHttpSession) session;
        Response response = new Response(Response.OK);
        response.addHeader("Transfer-Encoding: chunked");

        streamHttpSession.sendResponseSupplier(response, () -> rangeData.getChunk());
    }

    private void addTimeStamp(RequestParameters params) {
        Timestamp time = new Timestamp(System.currentTimeMillis());
        Record record = Record.of(Record.DUMMY, ByteBuffer.wrap(params.getBodyRequest()), time);
        params.setBodyRequest(record.getRawBytes());
    }

    public int getClusterSize() {
        return topologySize;
    }

    public int getQuorumCluster() {
        return quorumCluster;
    }

    public Response internalRequest(final Request request, final RequestParameters params) throws IOException {
        if (params.getStartKey().isEmpty()) {
            return badRequest();
        }
        final String host = request.getHost();
        boolean validHost = false;
        final Set<String> topology = replicationService.getTopology();
        for (final String node : topology) {
            if (node.contains(host)) {
                validHost = true;
                break;
            }
        }
        if (!validHost) {
            return badGateway();
        }
        return replicationService.directRequest(params);
    }

    public static Response badRequest() {
        return new Response(Response.BAD_REQUEST, Response.EMPTY);
    }

    public static Response badGateway() {
        return new Response(Response.BAD_GATEWAY, Response.EMPTY);
    }

    private boolean validParameter(final int ack, final int from) {
        return from <= getClusterSize() && ack > 0 && ack <= from;
    }
}
