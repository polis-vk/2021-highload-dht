package ru.mail.polis.service.alexander_kuptsov.sharding.time_measuring.measures;

import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.IDistributionAlgorithm;

import java.util.Set;

public class RemoveServerTimeMeasure extends TimeMeasure {
    private final int iterations;

    private static final int DEFAULT_ITERATIONS = 3;

    public RemoveServerTimeMeasure() {
        this(DEFAULT_ITERATIONS);
    }

    public RemoveServerTimeMeasure(int iterations) {
        this.iterations = iterations;
    }

    @Override
    protected double measure(IDistributionAlgorithm distributionAlgorithm, Set<String> topology) {
        long measuredTime = 0;
        for (int i = 0; i < iterations; i++) {
            distributionAlgorithm.addTopology(topology);
            for (String server : topology) {
                long startTime = System.nanoTime();
                distributionAlgorithm.removeServer(server);
                long stopTime = System.nanoTime();
                measuredTime = (measuredTime + (stopTime - startTime)) / 2;
            }
        }
        return measuredTime;
    }
}
