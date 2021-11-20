package ru.mail.polis.service.gasparyansokrat;

import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.lsm.DAO;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
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

    private CompletableFuture<Response> get(final String node, final String uri) {
        HttpRequest request = HttpRequest.newBuilder()
                                        .uri(URI.create(uri))
                                        .GET()
                                        .build();
        return clusterServers.get(node)
                .sendAsync(request, BodyHandlers.ofByteArray())
                .thenApply(response ->
                        new Response(FilterResponses.code2Str(response.statusCode()), response.body())
                );
    }

    private CompletableFuture<Response> put(final String node, final String uri,
                                                        final byte[] data) {
        HttpRequest request = HttpRequest.newBuilder()
                                         .uri(URI.create(uri))
                                         .PUT(HttpRequest.BodyPublishers.ofByteArray(data))
                                         .build();
        return clusterServers.get(node)
                .sendAsync(request, BodyHandlers.ofByteArray())
                .thenApply(response ->
                        new Response(FilterResponses.code2Str(response.statusCode()), response.body())
                );
    }

    private CompletableFuture<Response> delete(final String node, final String uri) {
        HttpRequest request = HttpRequest.newBuilder()
                                         .uri(URI.create(uri))
                                         .DELETE()
                                         .build();
        return clusterServers.get(node)
                .sendAsync(request, BodyHandlers.ofByteArray())
                .thenApply(response ->
                        new Response(FilterResponses.code2Str(response.statusCode()), response.body())
                );
    }

    private CompletableFuture<Response> externalRequest(final String id,
                                                                    final Request request, final String node) {
        final String uri = node + String.format(DAO_URI_PARAMETER, id);
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return get(node, uri);
            case Request.METHOD_PUT:
                return put(node, uri, request.getBody());
            case Request.METHOD_DELETE:
                return delete(node, uri);
            default:
                return ClusterService.asyncBadMethod();
        }
    }

    public Response handleRequest(final Request request, final int requireAck,
                                  final String id, final List<String> nodes) throws IOException {
        List<CompletableFuture<Response>> responses = new ArrayList<>();
        for (String node : nodes) {
            if (node.equals(selfNode)) {
                responses.add(serviceDAO.asyncHandleRequest(id, request));
            } else {
                responses.add(externalRequest(id, request, node));
            }
        }

        return FilterResponses.validResponse(request, responses, requireAck);
    }

    public Response directRequest(final String id, final Request request) throws IOException {
        return serviceDAO.handleRequest(id, request);
    }

}
