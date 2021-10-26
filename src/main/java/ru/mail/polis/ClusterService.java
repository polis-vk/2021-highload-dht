package ru.mail.polis;

import hash.HashFunction;

public class ClusterService {

    private final ClusterPartitioner clusterPartitioner;
    private final HashFunction hashFunction;

    public ClusterService(ClusterPartitioner clusterPartitioner, HashFunction hashFunction) {
        this.clusterPartitioner = clusterPartitioner;
        this.hashFunction = hashFunction;
    }

    public String getNodeForKey(String key) {
        int valueInRange = (int) (hashFunction.hash(key) & 0xfffffff) % ClusterPartitioner.RANGE_SIZE + 1;
        return clusterPartitioner.getNodeByValue(valueInRange);
    }

}
