package ru.mail.polis.service.sachuk.ilya.sharding;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.pool.PoolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;

public class NodeRouter {
    private final Logger logger = LoggerFactory.getLogger(NodeRouter.class);

    private final NodeManager nodeManager;

    public NodeRouter(NodeManager nodeManager) {
        this.nodeManager = nodeManager;
    }

    public Response route(Node currentNode, String key, Request request) {
        VNode vnode = nodeManager.getNearVNode(key);
        logger.info("in rout and port" + vnode.getPhysicalNode().port);

        logger.info("current port:" + currentNode.port + " and near port:" + vnode.getPhysicalNode().port);
        if (currentNode.port == vnode.getPhysicalNode().port) {
            logger.info("port is equal to this node" + currentNode.port + " " + vnode.getPhysicalNode().port);
            return null;
        }

        logger.info("port is not equal and routed");
        return routeToNode(vnode, request);
    }

    public Response routeToNode(VNode vnode, Request request) {
        String host = Node.HOST;
        int port = vnode.getPhysicalNode().port;

        HttpClient httpClient = nodeManager.getHttpClient(host + port);

        try {
            return httpClient.invoke(request);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (PoolException | HttpException e) {
            throw new IllegalStateException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
