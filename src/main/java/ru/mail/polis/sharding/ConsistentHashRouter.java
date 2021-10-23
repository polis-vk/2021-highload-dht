package ru.mail.polis.sharding;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Consistent hash router with virtual nodes support.
 *
 * @author Eldar Timraleev
 */
public class ConsistentHashRouter<T extends Node> implements HashRouter<T> {
    private final SortedMap<Long, VirtualNode> ring = new TreeMap<>();
    private final HashFunction hashFunction;

    public ConsistentHashRouter(@Nonnull Collection<T> nodes, int copiesOfEach) {
        this(nodes, copiesOfEach, new HashFunction.HashMD5());
    }

    public ConsistentHashRouter(@Nonnull Collection<T> nodes, int copiesOfEach, @Nonnull HashFunction hashFunction) {
        this.hashFunction = hashFunction;
        for (T node : nodes) {
            addNode(node, copiesOfEach);
        }
    }

    public void addNode(Node node, int copies) {
        int existingCount = countVirtualNodes(node);
        for (int i = 0; i < copies; i++) {
            VirtualNode virtualNode = new VirtualNode(node, existingCount + i);
            long hash = hashFunction.hash(virtualNode.getKey());
            ring.put(hash, virtualNode);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public T route(@Nonnull String key) {
        SortedMap<Long, VirtualNode> tailMap = ring.tailMap(hashFunction.hash(key));
        long virtualNodeKey = !tailMap.isEmpty() ? tailMap.firstKey() : ring.firstKey();
        return (T) ring.get(virtualNodeKey).node;
    }

    private int countVirtualNodes(Node node) {
        int counter = 0;
        for (VirtualNode virtualNode : ring.values()) {
            if (virtualNode.isWorkerFor(node)) {
                counter++;
            }
        }
        return counter;
    }
}
