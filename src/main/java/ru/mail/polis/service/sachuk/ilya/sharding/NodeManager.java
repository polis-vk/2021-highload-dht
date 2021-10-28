package ru.mail.polis.service.sachuk.ilya.sharding;

import one.nio.http.HttpClient;
import one.nio.net.ConnectionString;
import one.nio.util.Hash;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class NodeManager {
    @SuppressWarnings("PMD")
    private static volatile NodeManager instance;
    private static final Object LOCK_OBJECT = new Object();
    private final VNodeConfig vnodeConfig;
    private final NavigableMap<Integer, VNode> circle;
    private final NavigableMap<String, HttpClient> clients;
    private final AtomicInteger nodeCount = new AtomicInteger();

    private NodeManager(Set<String> topology, VNodeConfig vnodeConfig) {
        this.vnodeConfig = vnodeConfig;
        circle = new TreeMap<>();

        clients = new TreeMap<>();
        for (String endpoint : topology) {
            HttpClient client = new HttpClient(new ConnectionString(endpoint));
            clients.put(endpoint, client);
        }
    }

    @SuppressWarnings("PMD")
    public static NodeManager getInstance(Set<String> topology, VNodeConfig vnodeConfig) {
        if (instance == null) {
            synchronized (LOCK_OBJECT) {
                if (instance == null) {
                    instance = new NodeManager(topology, vnodeConfig);
                }
            }
        }

        return instance;
    }

    public void addNode(Node node) {
        nodeCount.incrementAndGet();
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

    public void removeNode() {
        nodeCount.decrementAndGet();

        if (nodeCount.get() == 0) {
            synchronized (LOCK_OBJECT) {
                instance = null;
            }
        }
    }
}
