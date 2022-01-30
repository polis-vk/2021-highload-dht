package ru.mail.polis.service.alexander_kuptsov.sharding.distribution;

import ru.mail.polis.service.alexander_kuptsov.sharding.hash.IHashAlgorithm;

import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

public class VNodesAlgorithm extends DistributionHashAlgorithm<IHashAlgorithm> {
    private final NavigableMap<Integer, Node> virtualNodes = new TreeMap<>();
    private final int virtualNodesCount;

    private static final int DEFAULT_VIRTUAL_NODES = 100;

    public VNodesAlgorithm(IHashAlgorithm hashAlgorithm) {
        this(hashAlgorithm, DEFAULT_VIRTUAL_NODES);
    }

    public VNodesAlgorithm(IHashAlgorithm hashAlgorithm, int virtualNodesCount) {
        super(hashAlgorithm);
        this.virtualNodesCount = virtualNodesCount;
    }

    @Override
    public void addTopology(Set<String> topology) {
        for (String server : topology) {
            addServer(server);
        }
    }

    @Override
    public void addServer(String server) {
        for (int i = 0; i < virtualNodesCount; i++) {
            Node node = new Node(server, i);
            int hash = getHash(node.getVirtualName());
            virtualNodes.put(hash, node);
        }
    }

    @Override
    public String getServer(String key) {
        int hash = getHash(key);
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

    @Override
    public void removeServer(String server) {
        int hash = getHash(server);
        virtualNodes.remove(hash);
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
