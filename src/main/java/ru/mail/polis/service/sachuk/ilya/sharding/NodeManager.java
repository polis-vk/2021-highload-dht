package ru.mail.polis.service.sachuk.ilya.sharding;

import one.nio.http.HttpClient;
import one.nio.net.ConnectionString;
import one.nio.util.Hash;

import java.io.Closeable;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

public final class NodeManager implements Closeable {
    private final NavigableMap<Integer, VNode> circle = new TreeMap<>();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final NavigableMap<String, HttpClient> clients;
    private final VNodeConfig vnodeConfig;

    public NodeManager(Set<String> topology, VNodeConfig vnodeConfig, Node node) {
        this.vnodeConfig = vnodeConfig;

        clients = new TreeMap<>();

        for (String endpoint : topology) {
            ConnectionString connectionString = new ConnectionString(endpoint);

            addNode(new Node(connectionString.getPort()));
            if (node.port == connectionString.getPort()) {
                continue;
            }

            HttpClient client = new HttpClient(new ConnectionString(endpoint));
            clients.put(endpoint, client);
        }
    }

    public VNode getNearVNode(String key) {
        Map.Entry<Integer, VNode> integerVNodeEntry = circle.ceilingEntry(Hash.murmur3(key));

        checkIsClosed();

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

    @Override
    public void close() {
        isClosed.set(true);
        for (HttpClient httpClient : clients.values()) {
            httpClient.close();
        }
    }

    private void addNode(Node node) {
        checkIsClosed();

        for (int i = 0; i < vnodeConfig.nodeWeight; i++) {
            int hashCode = Hash.murmur3(Node.HOST + node.port + i);

            circle.put(hashCode, new VNode(node));
        }
    }

    private void checkIsClosed() {
        if (isClosed.get()) {
            throw new IllegalStateException("NodeManager is already closed");
        }
    }
}
