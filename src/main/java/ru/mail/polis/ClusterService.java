package ru.mail.polis;

import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class ClusterService {

    public static final int RANGE_SIZE = 255;

    private final SortedMap<Integer, String> nodes;

    public ClusterService(Set<String> topology) {
        nodes = createPartitions(topology);
    }

    private SortedMap<Integer, String> createPartitions(Set<String> topology) {
        SortedMap<Integer, String> sortedMap = new TreeMap<>();
        topology.forEach(node -> sortedMap.put(hashValue(node), node));
        return sortedMap;
    }

    public int hashValue(String value) {

        final int p = 167_776_19;
        int hash = (int) 216_613_626_1L;
        for (int i = 0; i < value.length(); i++) {
            hash = (hash ^ value.charAt(i)) * p;
        }
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;

        return hash < 0 ? hash & 0xfffffff : hash;
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
