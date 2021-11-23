package ru.mail.polis.service.gasparyansokrat;

import one.nio.util.Hash;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashImpl implements ConsistentHash {

    private final SortedMap<Integer, String> domainNodes;
    private final FowlerNollVoHash hashFunc;

    public ConsistentHashImpl(Set<String> topology) {
        this.hashFunc = new FowlerNollVoHash();
        this.domainNodes = new TreeMap<>();
        for (final String node : topology) {
            int hashValue = hashFunc.hash(node.getBytes(StandardCharsets.UTF_8));
            domainNodes.put(hashValue, node);
        }
    }

    @Override
    public String getNode(final String key) {
        if (domainNodes.isEmpty()) {
            return "";
        }
        int hashValue = hashFunc.hash(key.getBytes(StandardCharsets.UTF_8));
        if (!domainNodes.containsKey(hashValue)) {
            SortedMap<Integer, String> tailMap = domainNodes.tailMap(hashValue);
            hashValue = tailMap.isEmpty() ? domainNodes.firstKey() : tailMap.firstKey();
        }
        return domainNodes.get(hashValue);
    }

    @Override
    public List<String> getNodes(final String key, final int numNodes) {
        if (domainNodes.isEmpty() || numNodes > domainNodes.size()) {
            return new ArrayList<>();
        }
        List<String> nodes = new ArrayList<>();
        int hashValue = hashFunc.hash(key.getBytes(StandardCharsets.UTF_8));
        Iterator<Integer> itHashValue = domainNodes.tailMap(hashValue).keySet().iterator();
        String node;
        while (nodes.size() != numNodes) {
            if (itHashValue.hasNext()) {
                node = domainNodes.get(itHashValue.next());
            } else {
                itHashValue = domainNodes.keySet().iterator();
                node = domainNodes.get(itHashValue.next());
            }
            nodes.add(node);
        }

        return nodes;
    }

    private int applyMultipleHash(final String key) {
        Integer hashValue = Hash.murmur3(key);
        hashValue = hashValue.hashCode();
        return hashFunc.hash(hashValue.toString().getBytes(StandardCharsets.UTF_8));
    }
}
