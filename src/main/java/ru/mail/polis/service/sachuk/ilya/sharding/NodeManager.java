package ru.mail.polis.service.sachuk.ilya.sharding;

import one.nio.http.HttpClient;
import one.nio.net.ConnectionString;
import one.nio.util.Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.service.sachuk.ilya.Pair;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

public final class NodeManager implements Closeable {
    private Logger logger = LoggerFactory.getLogger(NodeManager.class);
    private final NavigableMap<Integer, VNode> circle = new TreeMap<>();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final NavigableMap<String, HttpClient> clients;
    private final VNodeConfig vnodeConfig;
    private final Node node;

    public NodeManager(Set<String> topology, VNodeConfig vnodeConfig, Node node) {
        this.vnodeConfig = vnodeConfig;
        this.node = node;

        clients = new TreeMap<>();

        for (String endpoint : topology) {
            ConnectionString connectionString = new ConnectionString(endpoint);

            logger.info("port from constructor:" + connectionString.getPort());
            addNode(new Node(connectionString.getPort()));
            if (node.port == connectionString.getPort()) {
                continue;
            }

            HttpClient client = new HttpClient(new ConnectionString(endpoint));
            clients.put(endpoint, client);
        }
    }

    public Pair<Integer, VNode> getNearVNodeWithGreaterHash(String key, Integer hash, List<Integer> currentPorts) {
        checkIsClosed();

        logger.info("CIRCLE SIZE:" + circle.size());
        logger.info("hash:" + hash);
        Map.Entry<Integer, VNode> integerVNodeEntry;
        if (hash == null) {
            integerVNodeEntry = circle.higherEntry(Hash.murmur3(key));
        } else {
            integerVNodeEntry = circle.higherEntry(hash);
        }


        VNode vnode;
        Integer hashReturn;
        if (integerVNodeEntry == null) {
            logger.info("in if");
            vnode = circle.firstEntry().getValue();
            hashReturn = circle.firstEntry().getKey();
        } else {
            logger.info("in else");
            vnode = integerVNodeEntry.getValue();
            hashReturn = integerVNodeEntry.getKey();
        }

        logger.info("curr port:" + vnode.getPhysicalNode().port);

        if (currentPorts.contains(vnode.getPhysicalNode().port)) {
            return getNearVNodeWithGreaterHash(key, hashReturn, currentPorts);
        }

        logger.info("FOUND HASH IS :" + hashReturn);

        return new Pair<>(hashReturn, vnode);
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

        logger.info("NODE PORT:" + node.port);
        logger.info("hashes:");
        for (int i = 0; i < vnodeConfig.nodeWeight; i++) {
            int hashCode = Hash.murmur3(Node.HOST + node.port + i);

//            logger.info(String.valueOf(hashCode));
            circle.put(hashCode, new VNode(node));
        }

        logger.info("first" + circle.firstKey());
        logger.info("last" + circle.lastKey());
        logger.info(String.valueOf(circle.keySet()));
    }

    private void checkIsClosed() {
        if (isClosed.get()) {
            throw new IllegalStateException("NodeManager is already closed");
        }
    }
}
