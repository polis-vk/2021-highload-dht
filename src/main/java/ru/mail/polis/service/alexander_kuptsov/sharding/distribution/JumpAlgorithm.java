package ru.mail.polis.service.alexander_kuptsov.sharding.distribution;

import ru.mail.polis.service.alexander_kuptsov.sharding.hash.IHashAlgorithm;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

public class JumpAlgorithm extends DistributionHashAlgorithm<IHashAlgorithm> {
    private int size;
    private final NavigableMap<Integer, String> storage;

    public JumpAlgorithm(IHashAlgorithm hashAlgorithm) {
        super(hashAlgorithm);
        this.storage = new TreeMap<>();
    }

    @Override
    public void addTopology(Set<String> topology) {
        for (String server : topology) {
            addServer(server);
        }
    }

    @Override
    public void addServer(String server) {
        int bucket = storage.size();
        storage.put(bucket, server);
        size++;
    }

    @Override
    public String getServer(String id) {
        int bucket = getBucket(id);
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
        if (bucket == null || bucket != size - 1) {
            return;
        }
        storage.remove(bucket);
        size--;
    }

    public int getBucket(String key) {
        return getJumpConsistentHash(getHash(key), size);
    }

    private int getJumpConsistentHash(int inputKey, int bucketsSize) {
        long b = -1;
        long j = 0;
        long key = inputKey;
        while (j < bucketsSize) {
            b = j;
            key = key * 2_862_933_555_777_941_757L + 1;
            j = (long) ((b + 1) * ((double) (1L << 31) / (double) ((key >> 33) + 1)));
        }
        return (int) b;
    }
}
