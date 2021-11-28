package ru.mail.polis.service.sachuk.ilya.sharding;

import one.nio.net.ConnectionString;
import one.nio.util.Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.service.sachuk.ilya.Pair;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

public final class NodeManager implements Closeable {
    private final Logger logger = LoggerFactory.getLogger(NodeManager.class);
    private final NavigableMap<Integer, VNode> circle = new TreeMap<>();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final VNodeConfig vnodeConfig;

    public NodeManager(Set<String> topology, VNodeConfig vnodeConfig) {
        this.vnodeConfig = vnodeConfig;


        for (String endpoint : topology) {
            ConnectionString connectionString = new ConnectionString(endpoint);
            logger.info(connectionString.toString());

            addNode(new Node(connectionString.getProtocol(), connectionString.getHost(), connectionString.getPort(),
                    connectionString.toString()));
        }
    }

    public Pair<Integer, VNode> getNearVNodeWithGreaterHash(String key, Integer hash, List<Integer> currentPorts) {
        checkIsClosed();

        Map.Entry<Integer, VNode> integerVNodeEntry;
        integerVNodeEntry = circle.higherEntry(Objects.requireNonNullElseGet(hash, () -> Hash.murmur3(key)));

        VNode vnode;
        Integer hashReturn;
        if (integerVNodeEntry == null) {
            vnode = circle.firstEntry().getValue();
            hashReturn = circle.firstEntry().getKey();
        } else {
            vnode = integerVNodeEntry.getValue();
            hashReturn = integerVNodeEntry.getKey();
        }

        if (currentPorts.contains(vnode.getPhysicalNode().port)) {
            return getNearVNodeWithGreaterHash(key, hashReturn, currentPorts);
        }

        return new Pair<>(hashReturn, vnode);
    }

    @Override
    public void close() {
        isClosed.set(true);

    }

    private void addNode(Node node) {
        checkIsClosed();

        for (int i = 0; i < vnodeConfig.nodeWeight; i++) {
            int hashCode = Hash.murmur3(node.connectionString + i);

            circle.put(hashCode, new VNode(node));
        }
    }

    private void checkIsClosed() {
        if (isClosed.get()) {
            throw new IllegalStateException("NodeManager is already closed");
        }
    }
}
