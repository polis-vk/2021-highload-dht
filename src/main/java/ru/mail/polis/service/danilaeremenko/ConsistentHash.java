package ru.mail.polis.service.danilaeremenko;

import com.google.common.primitives.UnsignedLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.google.common.primitives.UnsignedLong.fromLongBits;
import static com.google.common.primitives.UnsignedLong.ONE;
import static com.google.common.primitives.UnsignedLong.valueOf;

public class ConsistentHash {
    private final SortedMap<Integer, ClusterAdapter> partitionMap = new TreeMap<>();
    private static final Logger C_HASH_LOGGING = LoggerFactory.getLogger(ConsistentHash.class);

    /*
    John Lamping, Eric Veach "A Fast, Minimal Memory, Consistent Hash Algorithm"
    https://arxiv.org/pdf/1406.2294.pdf
    */
    private static final UnsignedLong C_1 = valueOf(2_862_933_555_777_941_757L);
    private static final long C_2 = 1L << 31;
    private final int bucketSize;

    private int hashFunc(String str) {
        long keyHash = str.hashCode();
        UnsignedLong key = fromLongBits(keyHash);

        long b = -1;
        long j = 0;
        while (j < bucketSize) {
            b = j;

            key = key.times(C_1).plus(ONE);
            UnsignedLong keyShift = fromLongBits(key.longValue() >>> 33).plus(ONE);

            j = (long) ((b + 1) * (C_2 / keyShift.doubleValue()));
        }

        return (int) b;
    }

    public ConsistentHash(final Set<String> topology) {
        bucketSize = topology.size();
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
