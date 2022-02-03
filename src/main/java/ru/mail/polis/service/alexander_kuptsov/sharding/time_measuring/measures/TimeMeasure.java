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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

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
        List<String> endpoints = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            int port;
            do {
                port = randomPort();
            } while (endpoints.contains(endpoint(port)));
            endpoints.add(endpoint(port));
        }
        return endpoints;
    }

    protected static int randomPort() {
        try (ServerSocket socket = new ServerSocket()) {
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress(InetAddress.getLocalHost(), 0), 1);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Can't discover a free port", e);
        }
    }

    protected static String randomId() {
        return Long.toHexString(ThreadLocalRandom.current().nextLong());
    }

    private static String endpoint(final int port) {
        return "http://localhost:" + port;
    }
}
