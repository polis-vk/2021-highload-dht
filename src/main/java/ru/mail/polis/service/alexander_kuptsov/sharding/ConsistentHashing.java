package ru.mail.polis.service.alexander_kuptsov.sharding;

import ru.mail.polis.service.alexander_kuptsov.sharding.hash.IHashAlgorithm;

import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

public class ConsistentHashing {
    private final Set<String> topology;
    private final NavigableMap<Integer, Node> virtualNodes = new TreeMap<>();
    private final int virtualNodesCount;

    private final IHashAlgorithm hashAlgorithm;

    private static final int DEFAULT_VIRTUAL_NODES = 10;

    private ConsistentHashing(Set<String> topology, IHashAlgorithm hashAlgorithm, int virtualNodesCount) {
        this.topology = topology;
        this.virtualNodesCount = virtualNodesCount;
        this.hashAlgorithm = hashAlgorithm;
    }

    public static ConsistentHashing createByTopology(
            Set<String> topology,
            IHashAlgorithm hashAlgorithm) {
        return createByTopology(topology, hashAlgorithm, DEFAULT_VIRTUAL_NODES);
    }

    public static ConsistentHashing createByTopology(
            Set<String> topology,
            IHashAlgorithm hashAlgorithm,
            int virtualNodesCount) {
        ConsistentHashing consistentHashing = new ConsistentHashing(topology, hashAlgorithm, virtualNodesCount);
        consistentHashing.init();
        return consistentHashing;
    }

    private void init() {
        for (String server : topology) {
            for (int i = 0; i < virtualNodesCount; i++) {
                Node node = new Node(server, i);
                int hash = hashAlgorithm.getHash(node.getVirtualName());
                virtualNodes.put(hash, node);
            }
        }
    }

    public String getServer(String key) {
        int hash = hashAlgorithm.getHash(key);
        NavigableMap<Integer, Node> subMap = (NavigableMap<Integer, Node>) virtualNodes.tailMap(hash);
        Node virtualNode;
        if (subMap.isEmpty()) {
            // If there is no nodes, take the first node of the ring
            int i = virtualNodes.firstKey();
            virtualNode = virtualNodes.get(i);
        } else {
            int i = subMap.firstKey();
            virtualNode = subMap.get(i);
        }

        if (virtualNode != null) {
            return virtualNode.getServerName();
        }
        return null;
    }

    private static final class Node {
        private final String server;
        private final String virtualName;

        private static final String VIRTUAL_TOKEN = "_VN_";

        public Node(String server, int virtualIndex) {
            this.server = server;
            this.virtualName = server + VIRTUAL_TOKEN + virtualIndex;
        }

        public String getVirtualName() {
            return virtualName;
        }

        public String getServerName() {
            return server;
        }
    }
}
