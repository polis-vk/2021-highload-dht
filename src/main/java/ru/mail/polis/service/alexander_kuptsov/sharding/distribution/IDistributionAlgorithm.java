package ru.mail.polis.service.alexander_kuptsov.sharding.distribution;

import java.util.Set;

public interface IDistributionAlgorithm {
    void addTopology(Set<String> topology);

    void addServer(String server);

    String getServer(String id);

    void removeServer(String server);

    default void removeTopology(Set<String> topology) {
        for (String server : topology) {
            removeServer(server);
        }
    }
}
