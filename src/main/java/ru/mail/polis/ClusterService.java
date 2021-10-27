package ru.mail.polis;

import ru.mail.polis.hash.HashFunction;

import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class ClusterService {

    public static final int RANGE_SIZE = 255;

    private final SortedMap<Integer, String> nodes;

    private final HashFunction hashFunction;

    public ClusterService(Set<String> topology, HashFunction hashFunction) {
        this.hashFunction = hashFunction;

        nodes = createPartitions(topology);
    }

    private SortedMap<Integer, String> createPartitions(Set<String> topology) {
        SortedMap<Integer, String> sortedMap = new TreeMap<>();
        topology.forEach(node -> sortedMap.put(hashValue(node), node));
        return sortedMap;
    }

    public int hashValue(String value) {
        return (int) (hashFunction.hash(value) & 0xfffffff) % RANGE_SIZE + 1;
    }

    public String getNodeByValue(String value) {
        int hashedValue = hashValue(value);
        SortedMap<Integer, String> subMap = nodes.tailMap(hashedValue);
        Integer idx;
        if (subMap.isEmpty()) {
            idx = nodes.firstKey();
        } else {
            idx = subMap.firstKey();
        }
        return nodes.get(idx);
    }
}
