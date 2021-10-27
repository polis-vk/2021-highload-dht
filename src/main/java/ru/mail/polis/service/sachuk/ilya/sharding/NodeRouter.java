package ru.mail.polis.service.sachuk.ilya.sharding;

import one.nio.http.HttpClient;
import one.nio.net.ConnectionString;

public class NodeRouter {

    private final NodeManager nodeManager;

    public NodeRouter(NodeManager nodeManager) {
        this.nodeManager = nodeManager;
    }

    public void route(String key) {
        VNode vnode = nodeManager.getNearVNode(key);

        routeToNode(vnode);
    }

    private void routeToNode(VNode vNode) {
        String host = vNode.getPhysicalNode().host;
        int port = vNode.getPhysicalNode().port;
        HttpClient httpClient = new HttpClient(new ConnectionString(host + port)).;

    }
}
