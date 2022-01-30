package ru.mail.polis.service.alexander_kuptsov.sharding;

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

public final class TimeMeasuring {
    private static final Logger logger = LoggerFactory.getLogger(TimeMeasuring.class);
    private static final int COUNT_OF_KEYS = 100000;
    private static final int[] NUMBER_OF_NODES = new int[]{4, 16, 64, 256, 1024};
    private static final IHashAlgorithm DEFAULT_HASH_ALGORITHM = new Fnv1Algorithm();
    private static final IDistributionAlgorithm[] ALGORITHMS = new IDistributionAlgorithm[]{
            new AnchorAlgorithm(DEFAULT_HASH_ALGORITHM, 1024),
            new JumpAlgorithm(DEFAULT_HASH_ALGORITHM),
            new MaglevAlgorithm(DEFAULT_HASH_ALGORITHM),
            new MultiProbeAlgorithm(DEFAULT_HASH_ALGORITHM),
            new RendezvousAlgorithm(DEFAULT_HASH_ALGORITHM),
            new VNodesAlgorithm(DEFAULT_HASH_ALGORITHM)
    };

    private TimeMeasuring() {
    }

    public static void main(String[] args) {
       collectServerByIdResults();
       collectAddServerResults();
       collectRemoveServerResults();
    }

    private static void collectServerByIdResults() {
        HashMap<Integer, HashMap<String, Double>> resultsServerById = new HashMap<>();
        for (int numberOfNodes : NUMBER_OF_NODES) {
            Set<String> topology = new HashSet<>(getRandomNodes(numberOfNodes));
            HashMap<String, Double> currentResults = new HashMap<>();
            for (IDistributionAlgorithm algorithm : ALGORITHMS) {
                double measuredTime = measureServerById(algorithm, topology);
                currentResults.put(algorithm.getClass().getSimpleName(), measuredTime);
            }
            resultsServerById.put(numberOfNodes, currentResults);
        }
        logger.debug(resultsServerById.toString());
    }

    private static void collectAddServerResults() {
        HashMap<Integer, HashMap<String, Double>> resultsRemoveServer = new HashMap<>();
        for (int numberOfNodes : NUMBER_OF_NODES) {
            HashMap<String, Double> currentResults = new HashMap<>();
            for (IDistributionAlgorithm algorithm : ALGORITHMS) {
                double measuredTime = measureAddServer(algorithm);
                currentResults.put(algorithm.getClass().getSimpleName(), measuredTime);
            }
            resultsRemoveServer.put(numberOfNodes, currentResults);
        }
        logger.debug(resultsRemoveServer.toString());
    }

    private static void collectRemoveServerResults() {
        HashMap<Integer, HashMap<String, Double>> resultsAddServer = new HashMap<>();
        for (int numberOfNodes : NUMBER_OF_NODES) {
            Set<String> topology = new HashSet<>(getRandomNodes(numberOfNodes));
            HashMap<String, Double> currentResults = new HashMap<>();
            for (IDistributionAlgorithm algorithm : ALGORITHMS) {
                double measuredTime = measureRemoveServer(algorithm, topology);
                currentResults.put(algorithm.getClass().getSimpleName(), measuredTime);
            }
            resultsAddServer.put(numberOfNodes, currentResults);
        }
        logger.debug(resultsAddServer.toString());
    }

    private static double measureServerById(IDistributionAlgorithm distributionAlgorithm, Set<String> topology) {
        distributionAlgorithm.addTopology(topology);
        long measuredTime = 0;
        for (int j = 0; j < COUNT_OF_KEYS; j++) {
            long startTime = System.nanoTime();
            distributionAlgorithm.getServer(randomId());
            long stopTime = System.nanoTime();
            measuredTime = (measuredTime + (stopTime - startTime)) / 2;
        }
        return measuredTime;
    }

    private static double measureAddServer(IDistributionAlgorithm distributionAlgorithm) {
        final int startTopologySize = 16;
        final int iterations = 30;
        List<String> topology = getRandomNodes(startTopologySize + iterations);
        HashSet<String> startTopology = new HashSet<>(topology.subList(0, startTopologySize - 1));
        distributionAlgorithm.addTopology(startTopology);
        long measuredTime = 0;
        for (int j = 0; j < iterations; j++) {
            String server = topology.get(startTopologySize + j);
            long startTime = System.nanoTime();
            distributionAlgorithm.addServer(server);
            long stopTime = System.nanoTime();
            measuredTime = (measuredTime + (stopTime - startTime)) / 2;
        }
        return measuredTime;
    }

    private static double measureRemoveServer(IDistributionAlgorithm distributionAlgorithm, Set<String> topology) {
        distributionAlgorithm.addTopology(topology);
        long measuredTime = 0;
        for (String server : topology) {
            long startTime = System.nanoTime();
            distributionAlgorithm.removeServer(server);
            long stopTime = System.nanoTime();
            measuredTime = (measuredTime + (stopTime - startTime)) / 2;
        }
        return measuredTime;
    }

    private static List<String> getRandomNodes(final int size) {
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

    private static int randomPort() {
        try (ServerSocket socket = new ServerSocket()) {
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress(InetAddress.getLocalHost(), 0), 1);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Can't discover a free port", e);
        }
    }

    private static String randomId() {
        return Long.toHexString(ThreadLocalRandom.current().nextLong());
    }

    private static String endpoint(final int port) {
        return "http://localhost:" + port;
    }
}
