package ru.mail.polis.service.alexander_kuptsov.sharding;


import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.*;
import ru.mail.polis.service.alexander_kuptsov.sharding.hash.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class TimeMeasuring {
    private static final int COUNT_OF_KEYS = 100000;
    protected static final int[] NUMBER_OF_NODES = new int[]{4, 16, 64, 256, 1024};
    private static final IHashAlgorithm DEFAULT_HASH_ALGORITHM = new Fnv1Algorithm();
    private static final IDistributionAlgorithm[] ALGORITHMS = new IDistributionAlgorithm[]{
            new AnchorAlgorithm(DEFAULT_HASH_ALGORITHM, 1024),
            new JumpAlgorithm(DEFAULT_HASH_ALGORITHM),
            new MaglevAlgorithm(DEFAULT_HASH_ALGORITHM),
            new MultiProbeAlgorithm(DEFAULT_HASH_ALGORITHM),
            new RendezvousAlgorithm(DEFAULT_HASH_ALGORITHM),
            new VNodesAlgorithm(DEFAULT_HASH_ALGORITHM)
    };


    public static void main(String[] args) {
        HashMap<Integer, HashMap<String, Double>> results = new HashMap<>();
        for (int numberOfNodes : NUMBER_OF_NODES) {
            System.out.println("----- " + numberOfNodes);
            Set<String> topology = new HashSet<>(getRandomNodes(numberOfNodes));
            HashMap<String, Double> currentResults = new HashMap<>();
            for (IDistributionAlgorithm algorithm : ALGORITHMS) {
                System.out.println("--- " + algorithm.getClass().getSimpleName());
                double measuredTime = measureServerById(algorithm, topology);
                System.out.println(measuredTime);
                currentResults.put(algorithm.getClass().getSimpleName(), measuredTime);
            }
            results.put(numberOfNodes, currentResults);
        }
        System.out.println(results);
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

    private static double measureAddServer(IDistributionAlgorithm distributionAlgorithm, int startTopologySize) {
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
            break;
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
            socket.bind(new InetSocketAddress(InetAddress.getByName("0.0.0.0"), 0), 1);
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
