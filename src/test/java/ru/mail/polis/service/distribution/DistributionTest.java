package ru.mail.polis.service.distribution;

import ru.mail.polis.TestBase;
import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.*;
import ru.mail.polis.service.alexander_kuptsov.sharding.hash.*;

import java.util.*;

public abstract class DistributionTest<T extends IDistributionAlgorithm> extends TestBase {
    protected static final int COUNT_OF_KEYS = 100000;
    protected static final IHashAlgorithm DEFAULT_HASH_ALGORITHM = new Fnv1Algorithm();

    protected abstract T getAlgorithm();

    protected static Set<String> getRandomNodes(final int size) {
        Set<String> endpoints = new HashSet<>();
        for (int i = 0; i < size; i++) {
            int port;
            do {
                port = randomPort();
            } while (endpoints.contains(endpoint(port)));
            endpoints.add(endpoint(port));
        }
        return endpoints;
    }
}
