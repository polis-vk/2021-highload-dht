package ru.mail.polis;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class ClusterPartitioner {

    public static final int RANGE_SIZE = 255;

    private Map<String, Partition> nodeMap;
    private int nodeAmount;

    private ClusterPartitioner() {}

    // In real world we will use config system to set partition for each node
    public static ClusterPartitioner create(Set<String> topology) {
        ClusterPartitioner clusterPartitioner = new ClusterPartitioner();
        Iterator<String> iterator = topology.iterator();
        String firstNode = iterator.next();
        String secondNode = iterator.next();
        clusterPartitioner.nodeMap = Map.of(
                firstNode, new Partition(0, RANGE_SIZE / 2),
                secondNode, new Partition(RANGE_SIZE / 2 + 1, RANGE_SIZE));
        clusterPartitioner.nodeAmount = topology.size();
        return clusterPartitioner;
    }

    /**
     *
     * @param value - 0-RANGE_SIZE ranged value
     * @return - null node not found
     */
    public String getNodeByValue(int value) {
        for (Map.Entry<String, Partition> nodeEntry : nodeMap.entrySet()) {
            Partition partition = nodeEntry.getValue();
            if (partition.inRange(value)) {
                return nodeEntry.getKey();
            }
        }
        return null;
    }

    public int getNodeAmount() {
        return nodeAmount;
    }

}
