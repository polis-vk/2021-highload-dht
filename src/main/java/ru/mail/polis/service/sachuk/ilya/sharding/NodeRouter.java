package ru.mail.polis.service.sachuk.ilya.sharding;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.pool.PoolException;

import java.io.IOException;
import java.io.UncheckedIOException;

public class NodeRouter {
    private final NodeManager nodeManager;

    public NodeRouter(NodeManager nodeManager) {
        this.nodeManager = nodeManager;
    }

    public Response routeToNode(VNode vnode, Request request) {
        String host = Node.HOST;
        int port = vnode.getPhysicalNode().port;

        HttpClient httpClient = nodeManager.getHttpClient(host + port);

        if (httpClient == null) {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }

        try {
            return httpClient.invoke(request);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (PoolException e) {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        } catch (HttpException e) {
            throw new IllegalStateException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
