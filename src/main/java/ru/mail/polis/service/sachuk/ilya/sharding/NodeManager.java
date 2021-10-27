package ru.mail.polis.service.sachuk.ilya.sharding;

import one.nio.http.HttpClient;
import one.nio.net.ConnectionString;
import one.nio.util.Hash;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

public final class NodeManager {
    @SuppressWarnings("PMD")
    private static volatile NodeManager instance;
    private final VNodeConfig vnodeConfig;
    private final NavigableMap<Integer, VNode> circle;
    private final NavigableMap<String, HttpClient> clients;

    private NodeManager(Set<String> topology, VNodeConfig vnodeConfig) {
        this.vnodeConfig = vnodeConfig;
        circle = new TreeMap<>();

        clients = new TreeMap<>();
        for (String endpoint : topology) {
            HttpClient client = new HttpClient(new ConnectionString(endpoint));
            clients.put(endpoint, client);
        }
    }

    @SuppressWarnings("Disable singleton is not safety")
    public static NodeManager getInstance(Set<String> topology, VNodeConfig vnodeConfig) {
        if (instance == null) {
            synchronized (NodeManager.class) {
                if (instance == null) {
                    instance = new NodeManager(topology, vnodeConfig);
                }
            }
        }

        return instance;
    }

    public void addNode(Node node) {
        for (int i = 0; i < vnodeConfig.nodeWeight; i++) {
            int hashCode = Hash.murmur3(Node.HOST + node.port + i);

            circle.put(hashCode, new VNode(node));
        }
    }

    public VNode getNearVNode(String key) {
        Map.Entry<Integer, VNode> integerVNodeEntry = circle.ceilingEntry(Hash.murmur3(key));

        VNode vnode;
        if (integerVNodeEntry == null) {
            vnode = circle.firstEntry().getValue();
        } else {
            vnode = integerVNodeEntry.getValue();
        }

        return vnode;
    }

    public HttpClient getHttpClient(String endpoint) {
        return clients.get(endpoint);
    }

    public void close() {
        instance = null;
    }
}
