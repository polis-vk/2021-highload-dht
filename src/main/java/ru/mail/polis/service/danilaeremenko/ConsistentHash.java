package ru.mail.polis.service.danilaeremenko;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHash {
    private final SortedMap<Integer, ClusterAdapter> partitionMap = new TreeMap<>();
    private static final Logger C_HASH_LOGGING = LoggerFactory.getLogger(ConsistentHash.class);

    //TODO update implementation (is a stub, I swear, I will implement another methods!)
    private int hashFunc(String str) {
        return str.hashCode();
    }

    public ConsistentHash(final Set<String> topology) {
        for (String topologyDesc : topology) {
            ClusterAdapter currCluster = ClusterAdapter.fromStringDesc(topologyDesc);
            int topologyHash = hashFunc(topologyDesc);
            partitionMap.put(topologyHash, currCluster);
            C_HASH_LOGGING.debug("PARTITIONING: {}, {}", topologyHash, topologyDesc);
        }
    }

    public ClusterAdapter getClusterAdapter(String recordKey) {
        int keyHash = hashFunc(recordKey);
        SortedMap<Integer, ClusterAdapter> rightNodesMap = partitionMap.tailMap(keyHash);
        final ClusterAdapter targetAdapter;
        if (rightNodesMap.isEmpty()) {
            //hash lies between last and first nodes
            Integer i = partitionMap.firstKey();
            targetAdapter = partitionMap.get(i);
        } else {
            //ordinary case
            Integer i = rightNodesMap.firstKey();
            targetAdapter = rightNodesMap.get(i);
        }
        C_HASH_LOGGING.debug("KEY_HASH = {}, TARGET_NODE = {}", keyHash, targetAdapter.toURL());
        return targetAdapter;
    }
}
