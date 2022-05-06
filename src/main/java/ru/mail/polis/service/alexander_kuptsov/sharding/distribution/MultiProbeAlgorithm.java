package ru.mail.polis.service.alexander_kuptsov.sharding.distribution;

import ru.mail.polis.service.alexander_kuptsov.sharding.hash.IHashAlgorithm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class MultiProbeAlgorithm extends DistributionHashAlgorithm<IHashAlgorithm> {
    private final List<ServerProbe> probesRing;
    private final int numProbes;

    private static final int DEFAULT_NUM_PROBES = 21;

    public MultiProbeAlgorithm(IHashAlgorithm hashAlgorithm) {
        super(hashAlgorithm);
        this.numProbes = DEFAULT_NUM_PROBES;
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
        ServerProbe bucket = getProbe(server);
        int pos = Collections.binarySearch(probesRing, bucket);

        if (pos == probesRing.size()) {
            probesRing.add(bucket);
        } else {
            int index = pos < 0 ? -(pos + 1) : pos;
            probesRing.add(index, bucket);
        }
    }

    @Override
    public String getServer(String id) {
        int index = getIndex(id);
        return probesRing.get(index).server;
    }

    @Override
    public void removeServer(String server) {
        probesRing.removeIf(serverProbe -> serverProbe.server.equals(server));
    }

    private ServerProbe getProbe(String server) {
        long hash = getHash(server);
        return new ServerProbe(hash, server);
    }

    private int getIndex(String key) {
        int resIndex = 0;
        long minDistance = Long.MAX_VALUE;
        for (int i = 0; i < numProbes; i++) {
            long hash = getHashWithSeed(key, i);
            Probe probe = new Probe(hash);
            int currentIndex = Collections.binarySearch(probesRing, probe);
            if (currentIndex == -1) { // less than all elements - take first
                currentIndex = 0;
            }
            if (currentIndex == numProbes) { // greater than all elements - take last
                currentIndex--;
            }
            if (currentIndex < -1) { // shift index to the closest probe index (floor)
                currentIndex = -(currentIndex + 2);
            }

            long distance = probesRing.get(currentIndex).distance(hash);
            if (distance < minDistance) {
                minDistance = distance;
                resIndex = currentIndex;
            }
        }
        return resIndex;
    }

    private static class Probe implements Comparable<Probe> {
        public final long hash;

        public Probe(long hash) {
            this.hash = hash;
        }

        public long distance(long hash) {
            return Math.abs(this.hash - hash);
        }

        @Override
        public int compareTo(Probe other) {
            return Long.compare(this.hash, other.hash);
        }
    }

    private static class ServerProbe extends Probe {
        public final String server;

        public ServerProbe(long hash, String server) {
            super(hash);
            this.server = server;
        }
    }
}
