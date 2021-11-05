package ru.mail.polis.service.gasparyansokrat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Iterator;


public class ConsistentHashImpl implements ConsistentHash {

    private final int intervalSize;
    private final SortedMap<Integer, String> domainNodes;
    private final FowlerNollVoHash hashFunc;

    public ConsistentHashImpl(Set<String> topology, final int intervalSize) {
        this.intervalSize = intervalSize;
        this.hashFunc = new FowlerNollVoHash();
        this.domainNodes = new TreeMap<>();
        List<String> nodes = new ArrayList<>(topology);
        List<List<Integer>> shuffleNodes = ShuffleNodes(topology.size());
        int nodeIdx = 0;
        int listIdx = 0;
        List<Integer> listNode = shuffleNodes.get(listIdx);
        for (long start = 0; start < Integer.MAX_VALUE; start += intervalSize) {
            String node = nodes.get(listNode.get(nodeIdx));
            nodeIdx = nodeIdx + 1;
            if (nodeIdx == listNode.size()) {
                listIdx = (listIdx + 1) % shuffleNodes.size();
                listNode = shuffleNodes.get(listIdx);
            }
            nodeIdx = nodeIdx % topology.size();
            domainNodes.put((int)start, node);
        }
    }

    private List<List<Integer>> ShuffleNodes(final int size) {
        Integer[] numNodes = new Integer[size];
        for (int i = 0; i < size; ++i) {
            numNodes[i] = i;
        }
        Set<List<Integer>> permList = new HashSet<>();
        permute(numNodes, permList);
        return new ArrayList<>(permList);
    }

    private void permute(Integer[] nums, Set<List<Integer>> idxList) {
        permutation(0, nums.length - 1, nums, idxList);
    }

    private void permutation(int start, int end, Integer[] nums, Set<List<Integer>> idxList) {
        if (start == end) {
            idxList.add(new ArrayList<>(Arrays.asList(nums)));
        }
        for (int i = start; i <= end; i++) {
            idxList.add(swap(nums, start, i));
            permutation(start + 1, end, nums, idxList);
            idxList.add(swap(nums, start, i));
        }
    }

    private List<Integer> swap(Integer[] arr, int a, int b) {
        if (a == b) {
            return new ArrayList<>(Arrays.asList(arr));
        }
        Integer temp = arr[b];
        arr[b] = arr[a];
        arr[a] = temp;
        return new ArrayList<>(Arrays.asList(arr));
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


    @Override
    public List<String> getNodes(final String key, final int numNodes) throws IOException {
        Set<String> nodes = new HashSet<>();
        if (domainNodes.isEmpty() || numNodes > domainNodes.size()) {
            return null;
        }
        nodes.add(getNode(key));
        int curNumNodes = 1;
        int hashValue = hashFunc.hash(key.getBytes(StandardCharsets.UTF_8));
        long nodeIntervalValue = nextHalfInterval(hashValue);
        while(curNumNodes != numNodes) {
            SortedMap<Integer, String> tailMap = domainNodes.tailMap((int)nodeIntervalValue);
            Iterator<Integer> itHashValue;
            if (tailMap.isEmpty()) {
                itHashValue = domainNodes.keySet().iterator();
            } else {
                itHashValue = tailMap.keySet().iterator();
            }
            long nextInteval = findNode(nodes, itHashValue);
            curNumNodes += 1;
            nodeIntervalValue = nextHalfInterval(nodeIntervalValue + nextInteval);
        }

        return new ArrayList<>(nodes);
    }

    private long findNode(Set<String> nodes, Iterator<Integer> itHashValue) throws IOException {
        int limitSize = 1;
        String node = domainNodes.get(itHashValue.next());
        while(nodes.contains(node)) {
            if (limitSize < domainNodes.size()) {
                if (itHashValue.hasNext()) {
                    node = domainNodes.get(itHashValue.next());
                } else {
                    itHashValue = domainNodes.keySet().iterator();
                    node = domainNodes.get(itHashValue.next());
                }
                limitSize += 1;
            } else {
                throw new IOException("Not enough nodes in cluster");
            }
        }
        nodes.add(node);
        return (limitSize * intervalSize);
    }

    private long nextHalfInterval(final long hashValue) {
        return (hashValue  + (Integer.MAX_VALUE >> 1)) % Integer.MAX_VALUE;
    }
}
