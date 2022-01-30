package ru.mail.polis.service.alexander_kuptsov.sharding.distribution;

import ru.mail.polis.service.alexander_kuptsov.sharding.hash.IHashAlgorithm;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

public class AnchorAlgorithm extends DistributionHashAlgorithm<IHashAlgorithm> {
    private final Deque<Integer> removed;

    private int[] baseA;
    private int[] baseW;
    private int[] baseL;
    private int[] baseK;

    private int capacity;
    private int size;

    private NavigableMap<Integer, String> storage;

    private static final int DEFAULT_CAPACITY = 50;
    private static final int SEED = 0xAABBCCDD;

    public AnchorAlgorithm(IHashAlgorithm hashAlgorithm) {
        this(hashAlgorithm, DEFAULT_CAPACITY);
    }

    public AnchorAlgorithm(IHashAlgorithm hashAlgorithm, int capacity) {
        super(hashAlgorithm);
        this.removed = new ArrayDeque<>();
        this.capacity = capacity;
    }

    public void addTopology(Set<String> topology, int capacity) {
        this.capacity = capacity;
        addTopology(topology);
    }

    @Override
    public void addTopology(Set<String> topology) {
        this.size = topology.size();
        if (size >= capacity) {
            this.capacity = this.size * 2;
        }
        this.storage = new TreeMap<>();

        this.baseA = new int[capacity];
        this.baseW = new int[capacity];
        this.baseL = new int[capacity];
        this.baseK = new int[capacity];

        for (int i = 0; i < capacity; i++) {
            baseL[i] = i;
            baseW[i] = i;
            baseK[i] = i;
        }
        for (int i = capacity - 1; i >= size; i--) {
            baseA[i] = i;
        }

        int bucket = 0;
        for (String server : topology) {
            storage.put(bucket++, server);
            addServer(server);
        }
    }

    @Override
    public void addServer(String server) {
        if (size >= capacity) {
            throw new IndexOutOfBoundsException("No more space for new servers. Consider reassigning updated topology");
        }
        final int bucket = addBucket();
        try {
            storage.put(bucket, server);
        } catch (RuntimeException ex) {
            removeBucket(bucket);
            throw ex;
        }
    }

    @Override
    public String getServer(String id) {
        final int bucket = getBucket(id);
        return storage.get(bucket);
    }

    @Override
    public void removeServer(String server) {
        Integer bucket = null;
        for (Map.Entry<Integer, String> entry : storage.entrySet()) {
            if (Objects.equals(entry.getValue(), server)) {
                bucket = entry.getKey();
                break;
            }
        }
        if (bucket == null) {
            return;
        }
        removeBucket(bucket);
        storage.remove(bucket);
    }

    private int getBucket(String key) {
        int k = Math.abs(getHashWithSeed(key, SEED));
        int b = k % capacity;

        while (baseA[b] > 0) {
            k = Math.abs(getHashWithSeed(String.valueOf(k), b));
            int h = k % baseA[b];
            while (baseA[h] >= baseA[b]) {
                h = this.baseK[h];
            }
            b = h;
        }

        return b;
    }

    private int addBucket() {
        final int b = removed.isEmpty() ? size : removed.pop();
        baseA[b] = 0;
        baseL[baseW[size]] = size;
        baseW[baseL[b]] = b;
        baseK[b] = b;
        size++;
        return b;
    }

    private void removeBucket(int b) {
        size--;
        if (b < size || !removed.isEmpty()) {
            removed.push(b);
        }
        baseA[b] = size;
        baseW[baseL[b]] = baseW[size];
        baseL[baseW[size]] = baseL[b];
        baseK[b] = baseW[size];
    }
}
