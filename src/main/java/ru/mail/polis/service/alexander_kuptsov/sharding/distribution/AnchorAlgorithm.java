package ru.mail.polis.service.alexander_kuptsov.sharding.distribution;

import ru.mail.polis.service.alexander_kuptsov.sharding.hash.IHashAlgorithm;

import java.util.*;

public class AnchorAlgorithm extends DistributionHashAlgorithm<IHashAlgorithm> {
    private final Deque<Integer> removed;

    private int[] A;
    private int[] W;
    private int[] L;
    private int[] K;

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
        this.removed = new LinkedList<>();
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

        this.A = new int[capacity];
        this.W = new int[capacity];
        this.L = new int[capacity];
        this.K = new int[capacity];

        for (int i = 0; i < capacity; i++) {
            L[i] = i;
            W[i] = i;
            K[i] = i;
        }
        for (int i = capacity - 1; i >= size; i--)
            A[i] = i;

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

        while (A[b] > 0) {
            k = Math.abs(getHashWithSeed(String.valueOf(k), b));
            int h = k % A[b];
            while (A[h] >= A[b]) {
                h = K[h];
            }
            b = h;
        }

        return b;
    }

    private int addBucket() {
        final int b = removed.isEmpty() ? size : removed.pop();
        A[b] = 0;
        L[W[size]] = size;
        W[L[b]] = b;
        K[b] = b;
        size++;
        return b;
    }

    private void removeBucket(int b) {
        size--;
        if (b < size || !removed.isEmpty()) {
            removed.push(b);
        }
        A[b] = size;
        W[L[b]] = W[size];
        L[W[size]] = L[b];
        K[b] = W[size];
    }
}
