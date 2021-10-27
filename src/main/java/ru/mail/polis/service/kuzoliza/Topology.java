package ru.mail.polis.service.kuzoliza;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;

public class Topology {

    private final String[] nodes;
    private final String currentNode;

    public Topology(final Set<String> nodes, final String currentNode) {
        assert nodes.contains(currentNode);
        this.currentNode = currentNode;
        this.nodes = new String[nodes.size()];
        nodes.toArray(this.nodes);
        Arrays.sort(this.nodes);
    }

    public String getNode(ByteBuffer key) {
        HashCode hash = Hashing.goodFastHash(64).hashBytes(key);
        key.rewind();
        int nodeIndex = Hashing.consistentHash(hash, nodes.length);
        return nodes[nodeIndex];
    }

    public int size() {
        return nodes.length;
    }

    public boolean isCurrentNode(String node) {
        return node.equals(currentNode);
    }

    public String[] all() {
        return nodes.clone();
    }
}
