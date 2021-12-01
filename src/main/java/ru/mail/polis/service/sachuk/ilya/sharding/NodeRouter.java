package ru.mail.polis.service.sachuk.ilya.sharding;

import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.service.sachuk.ilya.ResponseUtils;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

public class NodeRouter {
    private final HttpClient httpClient;

    private final ExecutorService executor = new ForkJoinPool(16);

    public NodeRouter() {

        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(3))
                .executor(executor)
                .build();
    }

    public CompletableFuture<Response> routeToNode(VNode vnode, Request request) {

        return httpClient
                .sendAsync(getHttpRequest(request, vnode.getPhysicalNode()), HttpResponse.BodyHandlers.ofByteArray())
                .thenApplyAsync(this::httpResponseToResponse)
                .exceptionally(t -> new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
    }

    private Response httpResponseToResponse(HttpResponse<byte[]> httpResponse) {
        Response response = new Response(getResultCode(httpResponse.statusCode()), httpResponse.body());

        Map<String, List<String>> map = httpResponse.headers().map();

        map.forEach((k, v) -> {
            for (String s : v) {
                addHeaderToResponse(response, k, s);
            }
        });

        return response;
    }

    private void addHeaderToResponse(Response response, String header, String values) {
        response.addHeader(header + ": " + values);
    }

    private String getResultCode(int statusCode) {
        String resultCode;
        switch (statusCode) {
            case 200:
                resultCode = Response.OK;
                break;
            case 404:
                resultCode = Response.NOT_FOUND;
                break;
            case 201:
                resultCode = Response.CREATED;
                break;
            case 202:
                resultCode = Response.ACCEPTED;
                break;
            case 503:
                resultCode = Response.SERVICE_UNAVAILABLE;
                break;
            case 405:
                resultCode = Response.METHOD_NOT_ALLOWED;
                break;
            default:
                resultCode = Response.GATEWAY_TIMEOUT;
                break;
        }

        return resultCode;
    }

    public HttpRequest getHttpRequest(Request request, Node node) {
        String timestampHeader = ResponseUtils.TIMESTAMP_HEADER;
        timestampHeader = timestampHeader.trim();
        timestampHeader = timestampHeader.substring(0, timestampHeader.length() - 1);

        String timestampHeaderFromResponse = request.getHeader(ResponseUtils.TIMESTAMP_HEADER);

        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(node.connectionString + request.getURI()))
                .version(HttpClient.Version.HTTP_1_1)
                .timeout(Duration.ofSeconds(3));

        builder = timestampHeaderFromResponse == null
                ? builder
                : builder.setHeader(timestampHeader, timestampHeaderFromResponse);

        builder = builder.header("coordinator", "true");

        switch (request.getMethod()) {
            case Request.METHOD_GET:
                builder.GET();
                break;
            case Request.METHOD_PUT:
                builder.PUT(HttpRequest.BodyPublishers.ofByteArray(request.getBody()));
                break;
            case Request.METHOD_DELETE:
                builder.DELETE();
                break;
            default:
                throw new IllegalStateException("No such method");
        }

        return builder.build();
    }

}
