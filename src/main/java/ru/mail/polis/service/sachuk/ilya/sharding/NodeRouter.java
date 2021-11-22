package ru.mail.polis.service.sachuk.ilya.sharding;

import one.nio.http.Request;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.service.sachuk.ilya.ResponseUtils;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class NodeRouter {
    private Logger logger = LoggerFactory.getLogger(NodeRouter.class);
    private final NodeManager nodeManager;
    private static String schema = "http://localhost:";

    public NodeRouter(NodeManager nodeManager) {
        this.nodeManager = nodeManager;
    }

//    public Response routeToNode(VNode vnode, Request request) {
//        String host = Node.HOST;
//        int port = vnode.getPhysicalNode().port;
//
//        HttpClient httpClient = nodeManager.getHttpClient(host + port);
//
//        if (httpClient == null) {
//            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
//        }
//
//        try {
//            return httpClient.invoke(request);
//        } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//            throw new IllegalStateException(e);
//        } catch (PoolException e) {
//            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
//        } catch (HttpException e) {
//            throw new IllegalStateException(e);
//        } catch (IOException e) {
//            throw new UncheckedIOException(e);
//        }
//    }

    public CompletableFuture<Response> routeToNode(VNode vnode, Request request) {
        String host = Node.HOST;
        int port = vnode.getPhysicalNode().port;

        HttpClient httpClient = nodeManager.getHttpClient(host + port);

//        logger.info(httpClient.);

        logger.info("BEFORE sendAsync");

        try {
            httpClient.sendAsync(getHttpRequest(request, port), HttpResponse.BodyHandlers.ofByteArray())
    //                .thenApplyAsync(httpResponse -> {
    //                    httpResponse.headers().map().forEach();
    //                });
                    .thenAccept(httpResponse -> httpResponse.headers().map().forEach((k, v) -> logger.info("key is " + k + " and values is : " + v))).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        logger.info("AFTER sendAsync");

        return CompletableFuture.supplyAsync(() -> new Response(Response.OK, Response.EMPTY));
    }

    private Response httpResponseToResponse(HttpResponse<byte[]> httpResponse) {
        Response response = new Response(getResultCode(httpResponse.statusCode()), httpResponse.body());
        logger.info("body: " + httpResponse.body().length);
        Map<String, List<String>> map = httpResponse.headers().map();
//        map.forEach();
        return response;
    }

    private String getResultCode(int statusCode) {
        logger.info("status code is :" + statusCode);

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
        }

        logger.info("result code after switch: " + resultCode);

        return resultCode;
    }

    public HttpRequest getHttpRequest(Request request, int port) {

        logger.info("URI :" + request.getURI());
        String timestampHeaderFromResponse = request.getHeader(ResponseUtils.TIMESTAMP_HEADER);

        logger.info("Timestamp header: " + timestampHeaderFromResponse);

        URI uri = URI.create(schema + port + request.getURI());
        logger.info(uri.toString());

        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(uri)
                .version(HttpClient.Version.HTTP_1_1)
                .timeout(Duration.ofSeconds(3));

        String timestampHeader = ResponseUtils.TIMESTAMP_HEADER;
        timestampHeader = timestampHeader.trim();
        timestampHeader = timestampHeader.substring(0, timestampHeader.length() - 1);

        logger.info(timestampHeader);

        builder = timestampHeaderFromResponse == null ? builder : builder.setHeader(timestampHeader, timestampHeaderFromResponse);
        builder = builder.header("coordinator", "true");

//
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

        }

        HttpRequest httpRequest = builder.build();

        return httpRequest;
    }

}
