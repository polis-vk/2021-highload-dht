package ru.mail.polis.service.alexander_kuptsov.sharding.distribution;

import ru.mail.polis.service.alexander_kuptsov.sharding.hash.IHashAlgorithm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class MultiProbeAlgorithm extends DistributionHashAlgorithm<IHashAlgorithm> {
    private final List<Probe> probesRing;
    private final int numProbes;

    private static final int DEFAULT_NUM_PROBES = 21;

    public MultiProbeAlgorithm(IHashAlgorithm hashAlgorithm) {
        super(hashAlgorithm);
        this.numProbes = DEFAULT_NUM_PROBES;
        this.probesRing = new ArrayList<>();
    }

    public MultiProbeAlgorithm(IHashAlgorithm hashAlgorithm, int numProbes) {
        super(hashAlgorithm);
        this.numProbes = numProbes;
        this.probesRing = new ArrayList<>();
    }

    @Override
    public void addTopology(Set<String> topology) {
        for (String server : topology) {
            addServer(server);
        }
    }

    @Override
    public void addServer(String server) {
        final Probe bucket = getProbe(server);
        final int pos = Collections.binarySearch(probesRing, bucket);
        final int index = -(pos + 1);
        probesRing.add(index, bucket);
    }

    @Override
    public String getServer(String id) {
        final int index = getIndex(id);
        return probesRing.get(index).server;
    }

    @Override
    public void removeServer(String server) {
        final Probe bucket = getProbe(server);
        final int pos = Collections.binarySearch(probesRing, bucket);
        probesRing.remove(pos);
    }

    private Probe getProbe(String server) {
        final long hash = getHash(server);
        return new Probe(server, hash);
    }

    private int getIndex(String key) {
        int index = 0;
        long minDistance = Long.MAX_VALUE;
        for (int i = 0; i < numProbes; i++) {
            final long hash = getHashWithSeed(key, i);
            int low = 0;
            int high = probesRing.size();
            while (low < high) {
                final int mid = (low + high) >>> 1;
                if (probesRing.get(mid).hash > hash) {
                    high = mid;
                }
                else {
                    low = mid + 1;
                }
            }

            if (low >= probesRing.size()) {
                low = 0;
            }

            final long distance = probesRing.get(low).distance(hash);
            if (distance < minDistance) {
                minDistance = distance;
                index = low;
            }
        }

        return index;
    }

    private static class Probe implements Comparable<Probe> {
        final String server;
        final long hash;

        public Probe(String server, long hash) {
            this.server = server;
            this.hash = hash;
        }

        long distance(long hash) {
            return Math.abs(this.hash - hash);
        }

        @Override
        public int compareTo(Probe other) {
            return Long.compare(this.hash, other.hash);
        }
    }
}
