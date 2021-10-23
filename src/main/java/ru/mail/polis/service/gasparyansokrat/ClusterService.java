package ru.mail.polis.service.gasparyansokrat;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

public class ClusterService {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterService.class);
    private String selfNode;
    private final ConsistentHash clusterNodes;
    private final ServiceDAO servDAO;
    private final Map<String, HttpClient> clusterServers;
    private static final String DAO_URI_PARAMETER = "/v0/entity?id=";

    ClusterService(final DAO dao, final Set<String> topology, final ServiceConfig servConf) {
        this.servDAO = new ServiceDAO(dao);
        this.clusterNodes = new ConsistentHashImpl(topology, servConf.clusterIntervals);
        this.clusterServers = new Hashtable<>();
        buildTopology(servConf.port, topology);
    }

    private void buildTopology(final int port, final Set<String> topologies) {
        final String sport = String.valueOf(port);
        for (final String topology : topologies) {
            if (topology.contains(sport)) {
                this.selfNode = topology;
            } else {
                this.clusterServers.put(topology, new HttpClient(new ConnectionString(topology)));
            }
        }
    }

    private Response get(final String node, final String id) {
        try {
            return clusterServers.get(node).get(DAO_URI_PARAMETER + id);
        } catch (InterruptedException | PoolException | IOException | HttpException e) {
            LOG.error(e.getMessage());
            return new Response(Response.BAD_GATEWAY, Response.EMPTY);
        }
    }

    private Response put(final String node, final String id, final byte[] data) {
        try {
            return clusterServers.get(node).put(DAO_URI_PARAMETER + id, data);
        } catch (InterruptedException | PoolException | IOException | HttpException e) {
            LOG.error(e.getMessage());
            return new Response(Response.BAD_GATEWAY, Response.EMPTY);
        }
    }

    private Response delete(final String node, final String id) {
        try {
            return clusterServers.get(node).delete(DAO_URI_PARAMETER + id);
        } catch (InterruptedException | PoolException | IOException | HttpException e) {
            LOG.error(e.getMessage());
            return new Response(Response.BAD_GATEWAY, Response.EMPTY);
        }
    }

    private Response externalRequest(final int method, final String id, final Request request, final String node) {
        switch (method) {
            case Request.METHOD_GET:
                return this.get(node, id);
            case Request.METHOD_PUT:
                return this.put(node, id, request.getBody());
            case Request.METHOD_DELETE:
                return this.delete(node, id);
            default:
                return new Response(Response.METHOD_NOT_ALLOWED, "Bad request".getBytes(StandardCharsets.UTF_8));
        }
    }

    public Response handleRequest(final int method, final String id, final Request request) throws IOException {
        final String node = clusterNodes.getNode(id);
        if (node.equals(selfNode)) {
            return servDAO.handleRequest(method, id, request);
        } else {
            return externalRequest(method, id, request, node);
        }
    }

}
