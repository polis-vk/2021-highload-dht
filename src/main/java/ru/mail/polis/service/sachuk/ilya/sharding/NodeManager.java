package ru.mail.polis.service.sachuk.ilya.sharding;

import one.nio.http.HttpClient;
import one.nio.net.ConnectionString;
import one.nio.util.Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

public final class NodeManager implements Closeable {
    private final Logger logger = LoggerFactory.getLogger(NodeRouter.class);

    private final VNodeConfig vnodeConfig;
    private static final NavigableMap<Integer, VNode> CIRCLE = new ConcurrentSkipListMap<>();
    private final NavigableMap<String, HttpClient> clients;
    private final Node node;

    public NodeManager(Set<String> topology, VNodeConfig vnodeConfig, Node node) {
        this.vnodeConfig = vnodeConfig;
        this.node = node;

        clients = new TreeMap<>();

        for (String endpoint : topology) {
            ConnectionString connectionString = new ConnectionString(endpoint);
            if (node.port == connectionString.getPort()) {
                continue;
            }
            HttpClient client = new HttpClient(new ConnectionString(endpoint));
            clients.put(endpoint, client);
        }

        addNode(node);
    }

    private void addNode(Node node) {
        for (int i = 0; i < vnodeConfig.nodeWeight; i++) {
            int hashCode = Hash.murmur3(Node.HOST + node.port + i);

            CIRCLE.put(hashCode, new VNode(node));
        }
        logger.info("server with port in end add node:" + node.port);
    }

    public VNode getNearVNode(String key) {
        logger.info("in getNearNode");

        Map.Entry<Integer, VNode> integerVNodeEntry = CIRCLE.ceilingEntry(Hash.murmur3(key));

        VNode vnode;
        if (integerVNodeEntry == null) {
            vnode = CIRCLE.firstEntry().getValue();
        } else {
            vnode = integerVNodeEntry.getValue();
        }

        logger.info("in end getNearNode, key is: " + key);
        logger.info("in end getNearNode, found vNode and port: " + vnode.getPhysicalNode().port);


        return vnode;
    }

    public HttpClient getHttpClient(String endpoint) {
        Set<String> strings = clients.keySet();

        for (String string : strings) {
            logger.info("port from sortedMap: " + string);
            logger.info(String.valueOf(endpoint.equals(string)));
        }

        return clients.get(endpoint);
    }

    @Override
    public void close() {
        for (HttpClient httpClient : clients.values()) {
            httpClient.close();
        }

        for (Map.Entry<Integer, VNode> integerVNodeEntry : CIRCLE.entrySet()) {
            if (integerVNodeEntry.getValue().getPhysicalNode().port == node.port) {
                CIRCLE.remove(integerVNodeEntry.getKey());
            }
        }
    }
}
