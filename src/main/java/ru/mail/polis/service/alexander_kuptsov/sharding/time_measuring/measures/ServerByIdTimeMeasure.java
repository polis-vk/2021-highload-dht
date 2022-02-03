package ru.mail.polis.service.alexander_kuptsov.sharding.time_measuring.measures;

import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.IDistributionAlgorithm;

import java.util.Set;

public final class ServerByIdTimeMeasure extends TimeMeasure {
    private final int countOfKeys;

    private static final int DEFAULT_COUNT_OF_KEYS = 100_000;

    public ServerByIdTimeMeasure() {
        this(DEFAULT_COUNT_OF_KEYS);
    }

    public ServerByIdTimeMeasure(int countOfKeys) {
        this.countOfKeys = countOfKeys;
    }


    @Override
    protected double measure(IDistributionAlgorithm distributionAlgorithm, Set<String> topology) {
        distributionAlgorithm.addTopology(topology);
        long measuredTime = 0;
        for (int j = 0; j < countOfKeys; j++) {
            long startTime = System.nanoTime();
            distributionAlgorithm.getServer(randomId());
            long stopTime = System.nanoTime();
            measuredTime = (measuredTime + (stopTime - startTime)) / 2;
        }
        return measuredTime;
    }
}
