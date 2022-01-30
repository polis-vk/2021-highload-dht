package ru.mail.polis.service.alexander_kuptsov.sharding.distribution;

import ru.mail.polis.service.alexander_kuptsov.sharding.hash.IHashAlgorithm;

import java.util.HashSet;
import java.util.Set;

public class RendezvousAlgorithm extends DistributionHashAlgorithm<IHashAlgorithm> {
    private final Set<String> topology;

    public RendezvousAlgorithm(IHashAlgorithm hashAlgorithm) {
        super(hashAlgorithm);
        this.topology = new HashSet<>();
    }

    @Override
    public void addTopology(Set<String> topology) {
        this.topology.addAll(topology);
    }

    @Override
    public void addServer(String server) {
        this.topology.add(server);
    }

    @Override
    public String getServer(String id) {
        String currentServer = null;
        long maxHash = Long.MIN_VALUE;

        for (String server : topology) {
            final long hash = getHash(id + server);
            if (hash > maxHash) {
                currentServer = server;
                maxHash = hash;
            }
        }

        return currentServer;
    }

    @Override
    public void removeServer(String server) {
        topology.remove(server);
    }

    @Override
    public void removeTopology(Set<String> topology) {
        this.topology.removeAll(topology);
    }
}
