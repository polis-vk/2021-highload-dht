package ru.mail.polis.service.gasparyansokrat;

import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.lsm.DAO;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class ReplicationService {

    private final String selfNode;
    private final ServiceDAO serviceDAO;
    private final Map<String, HttpClient> clusterServers;

    private static final String DAO_URI_PARAMETER = "/internal/cluster/entity?id=%s";

    ReplicationService(final DAO dao, final String selfNode,
                        final Map<String, HttpClient> clusterServers) {
        this.serviceDAO = new ServiceDAO(dao);
        this.clusterServers = clusterServers;
        this.selfNode = selfNode;
    }

    public Set<String> getTopology() {
        return clusterServers.keySet();
    }

    private CompletableFuture<HttpResponse<byte[]>> get(final String node, final String uri) {
        final HttpRequest request = HttpRequest.newBuilder()
                                                .uri(URI.create(uri))
                                                .GET()
                                                .build();
        return clusterServers.get(node).sendAsync(request, BodyHandlers.ofByteArray());
    }

    private CompletableFuture<HttpResponse<byte[]>> put(final String node, final String uri,
                                                        final byte[] data) {
        final HttpRequest request = HttpRequest.newBuilder()
                                                .uri(URI.create(uri))
                                                .PUT(BodyPublishers.ofByteArray(data))
                                                .build();
        return clusterServers.get(node).sendAsync(request, BodyHandlers.ofByteArray());
    }

    private CompletableFuture<HttpResponse<byte[]>> delete(final String node, final String uri) {
        final HttpRequest request = HttpRequest.newBuilder()
                                                .uri(URI.create(uri))
                                                .DELETE()
                                                .build();
        return clusterServers.get(node).sendAsync(request, BodyHandlers.ofByteArray());
    }

    private CompletableFuture<HttpResponse<byte[]>> externalRequest(final String id,
                                                                    final RequestParameters params,
                                                                    final String node) {
        final String uri = node + String.format(DAO_URI_PARAMETER, id);
        switch (params.getHttpMethod()) {
            case Request.METHOD_GET:
                return get(node, uri);
            case Request.METHOD_PUT:
                return put(node, uri, params.getBodyRequest());
            case Request.METHOD_DELETE:
                return delete(node, uri);
            default:
                return null;
        }
    }

    public void handleRequest(final RequestParameters params,
                              final HttpSession session,
                              final List<String> nodes) throws IOException {
        List<CompletableFuture<HttpResponse<byte[]>>> responses = new ArrayList<>();
        for (String node : nodes) {
            if (node.equals(selfNode)) {
                CompletableFuture<HttpResponse<byte[]>> future = CompletableFuture.completedFuture(
                    WrapperHttpResponse.buildResponse(serviceDAO.handleRequest(params))
                );
                responses.add(future);
            } else {
                responses.add(externalRequest(params.getId(), params, node));
            }
        }

        FilterResponses.validResponse(params, session, responses);
    }

    public Response directRequest(final RequestParameters params) throws IOException {
        return serviceDAO.handleRequest(params);
    }

}
