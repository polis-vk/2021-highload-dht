package ru.mail.polis.service.alexander_kuptsov.sharding.time_measuring.measures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.AnchorAlgorithm;
import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.IDistributionAlgorithm;
import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.JumpAlgorithm;
import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.MaglevAlgorithm;
import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.MultiProbeAlgorithm;
import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.RendezvousAlgorithm;
import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.VNodesAlgorithm;
import ru.mail.polis.service.alexander_kuptsov.sharding.hash.Fnv1Algorithm;
import ru.mail.polis.service.alexander_kuptsov.sharding.hash.IHashAlgorithm;
import ru.mail.polis.service.alexander_kuptsov.sharding.time_measuring.TimeMeasuring;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public abstract class TimeMeasure {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeMeasuring.class);
    private static final int[] NUMBER_OF_NODES = {4, 16, 64, 256, 1024};
    private static final IHashAlgorithm DEFAULT_HASH_ALGORITHM = new Fnv1Algorithm();
    private static final IDistributionAlgorithm[] ALGORITHMS = {
            new AnchorAlgorithm(DEFAULT_HASH_ALGORITHM, 1024),
            new JumpAlgorithm(DEFAULT_HASH_ALGORITHM),
            new MaglevAlgorithm(DEFAULT_HASH_ALGORITHM),
            new MultiProbeAlgorithm(DEFAULT_HASH_ALGORITHM),
            new RendezvousAlgorithm(DEFAULT_HASH_ALGORITHM),
            new VNodesAlgorithm(DEFAULT_HASH_ALGORITHM)
    };
    private static final int MIN_ALLOWED_PORT = 1;
    private static final int MAX_ALLOWED_PORT = 65535;

    protected TimeMeasure() {
        // Default constructor
    }

    public void collectResults() {
        HashMap<Integer, HashMap<String, Double>> results = new HashMap<>();
        for (int numberOfNodes : NUMBER_OF_NODES) {
            Set<String> topology = new HashSet<>(getRandomNodes(numberOfNodes));
            HashMap<String, Double> currentResults = new HashMap<>();
            for (IDistributionAlgorithm algorithm : ALGORITHMS) {
                double measuredTime = measure(algorithm, topology);
                currentResults.put(algorithm.getClass().getSimpleName(), measuredTime);
            }
            results.put(numberOfNodes, currentResults);
        }
        LOGGER.debug(results.toString());
    }

    protected abstract double measure(IDistributionAlgorithm distributionAlgorithm, Set<String> topology);

    protected static List<String> getRandomNodes(final int size) {
        List<Integer> ports = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            int port;
            do {
                port = randomPort();
            } while (ports.contains(port));
            ports.add(port);
        }
        return ports
                .stream()
                .map(TimeMeasure::endpoint)
                .collect(Collectors.toList());
    }

    protected static int randomPort() {
        return MIN_ALLOWED_PORT + (int) (Math.random() * (MAX_ALLOWED_PORT - MIN_ALLOWED_PORT));
    }

    protected static String randomId() {
        return Long.toHexString(ThreadLocalRandom.current().nextLong());
    }

    private static String endpoint(final int port) {
        return "http://localhost:" + port;
    }
}
