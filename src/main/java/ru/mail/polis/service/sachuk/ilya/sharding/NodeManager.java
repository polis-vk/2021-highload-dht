package ru.mail.polis.service.sachuk.ilya.sharding;

import one.nio.http.HttpClient;
import one.nio.net.ConnectionString;
import one.nio.util.Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public final class NodeManager {
    private final Logger logger = LoggerFactory.getLogger(NodeRouter.class);

    @SuppressWarnings("PMD")
    private static volatile NodeManager instance;
    private final VNodeConfig vnodeConfig;
    private final NavigableMap<Integer, VNode> circle;
    private final SortedMap<String, HttpClient> clients;

    private NodeManager(Set<String> topology, VNodeConfig vNodeConfig) {
        this.vnodeConfig = vNodeConfig;
        circle = new TreeMap<>();

        clients = new TreeMap<>();
        for (String endpoint : topology) {
            HttpClient client = new HttpClient(new ConnectionString(endpoint));
            clients.put(endpoint, client);
        }
    }

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
        logger.info("server with port in add node:" + node.port);
        for (int i = 0; i < vnodeConfig.nodeWeight; i++) {
            int hashCode = Hash.murmur3(Node.HOST + node.port + i);

            circle.put(hashCode, new VNode(node));
        }
        logger.info("server with port in end add node:" + node.port);
    }

    public VNode getNearVNode(String key) {
        logger.info("in getNearNode");

        Map.Entry<Integer, VNode> integerVNodeEntry = circle.ceilingEntry(Hash.murmur3(key));

        VNode vNode;
        if (integerVNodeEntry == null) {
            vNode = circle.firstEntry().getValue();
        } else {
            vNode = integerVNodeEntry.getValue();
        }

        logger.info("in end getNearNode, key is: " + key);
        logger.info("in end getNearNode, found vNode and port: " + vNode.getPhysicalNode().port);

        return vNode;
    }

    public HttpClient getHttpClient(String endpoint) {
        Set<String> strings = clients.keySet();

        for (String string : strings) {
            logger.info("port from sortedMap: " + string);
            logger.info(String.valueOf(endpoint.equals(string)));
        }

        return clients.get(endpoint);
    }

    public void close() {
        instance = null;
    }
}
