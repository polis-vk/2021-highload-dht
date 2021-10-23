package ru.mail.polis.service.gasparyansokrat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashImpl implements ConsistentHash {

    private final SortedMap<Integer, String> domainNodes;
    private final FowlerNollVoHash hashFunc;

    public ConsistentHashImpl(Set<String> topology, final int numIntervals) {
        this.hashFunc = new FowlerNollVoHash();
        this.domainNodes = new TreeMap<>();
        List<String> nodes = new ArrayList<>(topology);
        int nodeIdx = 0;
        for (long start = 0; start < Integer.MAX_VALUE; start += numIntervals) {
            String node = nodes.get(nodeIdx);
            domainNodes.put((int)start, node);
            nodeIdx = (nodeIdx + 1) % topology.size();
        }
    }

    @Override
    public String getNode(final String key) {
        if (domainNodes.isEmpty()) {
            return null;
        }
        int hashValue = hashFunc.hash(key.getBytes(StandardCharsets.UTF_8));
        if (!domainNodes.containsKey(hashValue)) {
            SortedMap<Integer, String> tailMap = domainNodes.tailMap(hashValue);
            hashValue = tailMap.isEmpty() ? domainNodes.firstKey() : tailMap.firstKey();
        }
        return domainNodes.get(hashValue);
    }

}
