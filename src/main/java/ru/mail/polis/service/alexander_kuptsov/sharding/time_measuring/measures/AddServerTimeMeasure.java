package ru.mail.polis.service.alexander_kuptsov.sharding.time_measuring.measures;

import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.IDistributionAlgorithm;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AddServerTimeMeasure extends TimeMeasure {
    private final int iterations;

    private static final int DEFAULT_ITERATIONS = 30;

    public AddServerTimeMeasure() {
        this(DEFAULT_ITERATIONS);
    }

    public AddServerTimeMeasure(int iterations) {
        this.iterations = iterations;
    }

    @Override
    protected double measure(IDistributionAlgorithm distributionAlgorithm, Set<String> topology) {
        final int startTopologySize = topology.size();
        List<String> baseTopology = getRandomNodes(startTopologySize + iterations);
        HashSet<String> startTopology = new HashSet<>(baseTopology.subList(0, startTopologySize - 1));
        distributionAlgorithm.addTopology(startTopology);
        long measuredTime = 0;
        for (int j = 0; j < iterations; j++) {
            String server = baseTopology.get(startTopologySize + j);
            long startTime = System.nanoTime();
            distributionAlgorithm.addServer(server);
            long stopTime = System.nanoTime();
            measuredTime = (measuredTime + (stopTime - startTime)) / 2;
        }
        return measuredTime;
    }
}
