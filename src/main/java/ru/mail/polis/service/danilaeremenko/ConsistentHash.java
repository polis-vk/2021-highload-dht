package ru.mail.polis.service.danilaeremenko;

import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHash {
    private final SortedMap<Integer, ClusterAdapter> partitionMap = new TreeMap<>();

    //TODO update implementation (is a stub, I swear, I will implement another methods!)
    private int hashFunc(String str) {
        return str.hashCode();
    }

    public ConsistentHash(final Set<String> topology) {
        for (String topologyDesc : topology) {
            ClusterAdapter currCluster = ClusterAdapter.fromStringDesc(topologyDesc);
            partitionMap.put(hashFunc(topologyDesc), currCluster);
        }
    }

    public ClusterAdapter getClusterAdapter(String recordKey) {
        int keyHash = hashFunc(recordKey);
        SortedMap<Integer, ClusterAdapter> subMap = partitionMap.tailMap(keyHash);
        if (subMap.isEmpty()) {
            //hash lies between last and first nodes
            Integer i = partitionMap.firstKey();
            return partitionMap.get(i);
        } else {
            //ordinary case
            Integer i = subMap.firstKey();
            return subMap.get(i);
        }
    }
}
