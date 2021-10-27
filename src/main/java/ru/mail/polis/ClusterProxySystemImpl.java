package ru.mail.polis;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;

import java.io.IOException;

public class ClusterProxySystemImpl implements ClusterProxySystem {

    private final ClusterService clusterService;

    public ClusterProxySystemImpl(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public Response invokeEntityRequest(String entityId, Request request)
            throws HttpException, IOException, PoolException, InterruptedException {
        String nodeUrl = clusterService.getNodeByValue(entityId);
        try (HttpClient httpClient = new HttpClient(new ConnectionString(nodeUrl))) {
            request.addHeader("Proxied:true");
            return httpClient.invoke(request);
        }
    }
}
