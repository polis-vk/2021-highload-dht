package ru.mail.polis.service.sachuk.ilya.sharding;

import one.nio.http.HttpClient;
import one.nio.net.ConnectionString;
import one.nio.util.Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.service.sachuk.ilya.Pair;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public final class NodeManager implements Closeable {
    private Logger logger = LoggerFactory.getLogger(NodeManager.class);
    private final NavigableMap<Integer, VNode> CIRCLE = new TreeMap<>();
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

            addNode(new Node(connectionString.getPort()));
            if (node.port == connectionString.getPort()) {
                continue;
            }

            HttpClient client = new HttpClient(new ConnectionString(endpoint));
            clients.put(endpoint, client);
        }

//        addNode(node);
    }

    public VNode getNearVNode(String key) {
        Map.Entry<Integer, VNode> integerVNodeEntry = CIRCLE.ceilingEntry(Hash.murmur3(key));

        checkIsClosed();

        VNode vnode;
        if (integerVNodeEntry == null) {
            vnode = CIRCLE.firstEntry().getValue();
        } else {
            vnode = integerVNodeEntry.getValue();
        }

        return vnode;
    }

    public Pair<Integer, VNode> getNearVNodeWithGreaterHash(String key, Integer hash, List<Integer> currentPorts) {
        logger.info("hash:" + hash);
        Map.Entry<Integer, VNode> integerVNodeEntry;
        if (hash == null) {
            integerVNodeEntry = CIRCLE.higherEntry(Hash.murmur3(key));
        } else {
            integerVNodeEntry = CIRCLE.higherEntry(hash);
        }

//        checkIsClosed();

        VNode vnode;
        Integer hashReturn;
        if (integerVNodeEntry == null) {
            logger.info("in if");
            vnode = CIRCLE.firstEntry().getValue();
            hashReturn = CIRCLE.firstEntry().getKey();
        } else {
            logger.info("in else");
            vnode = integerVNodeEntry.getValue();
            hashReturn = integerVNodeEntry.getKey();
        }

        logger.info("curr port:" + vnode.getPhysicalNode().port);

        if (currentPorts.contains(vnode.getPhysicalNode().port)) {
            getNearVNodeWithGreaterHash(key, hashReturn, currentPorts);
        }

        logger.info("FOUND HASH IS :" + hashReturn);

        return new Pair<>(hashReturn, vnode);
    }

    public VNode getNearVnodeNotInList(String key, List<VNode> vnodeList) {
        logger.info("in getNEarVNodeNot... Circle list is:" + CIRCLE.size());
        if (vnodeList.size() == 0) {
            return getNearVNode(key);
        }

        int prevHash = 0;
        boolean first = true;
        while (true) {
            Map.Entry<Integer, VNode> entry;
            if (first) {
                entry = CIRCLE.ceilingEntry(Hash.murmur3(key));
            } else {
                entry = CIRCLE.ceilingEntry(prevHash);
            }

            VNode vnode;
            int hash;
            if (entry == null) {
                logger.info("in getNEarVNodeNot... Circle list is(in if entry == null):" + CIRCLE.size());
                hash = CIRCLE.firstEntry().getKey();
                vnode = CIRCLE.firstEntry().getValue();
            } else {
                hash = entry.getKey();
                vnode = entry.getValue();
            }

            List<Integer> ports = vnodeList.stream().map(nodes -> nodes.getPhysicalNode().port).collect(Collectors.toList());

            if (!ports.contains(vnode.getPhysicalNode().port)) {
                return vnode;
            }
            prevHash = hash;
            first = false;
        }
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

        List<Integer> vnodesToRemove = new ArrayList<>();
        for (Map.Entry<Integer, VNode> entry : CIRCLE.entrySet()) {
            VNode vnode = entry.getValue();
            if (vnode.getPhysicalNode().port == node.port) {
                vnodesToRemove.add(entry.getKey());
            }
        }

//        for (Integer hash : vnodesToRemove) {
//            CIRCLE.remove(hash);
//        }
    }

    private void addNode(Node node) {
        checkIsClosed();

        logger.info("NODE PORT:" + node.port);
        logger.info("hashes:");
        for (int i = 0; i < vnodeConfig.nodeWeight; i++) {
            int hashCode = Hash.murmur3(Node.HOST + node.port + i);

            logger.info(String.valueOf(hashCode));
            CIRCLE.put(hashCode, new VNode(node));
        }

        logger.info("first" + String.valueOf(CIRCLE.firstKey()));
        logger.info("last" + String.valueOf(CIRCLE.lastKey()));
        logger.info(String.valueOf(CIRCLE.keySet()));
    }

    private void checkIsClosed() {
        if (isClosed.get()) {
            throw new IllegalStateException("NodeManager is already closed");
        }
    }
}
