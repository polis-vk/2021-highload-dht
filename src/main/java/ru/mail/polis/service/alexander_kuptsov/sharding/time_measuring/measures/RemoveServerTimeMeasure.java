package ru.mail.polis.service.alexander_kuptsov.sharding.time_measuring.measures;

import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.IDistributionAlgorithm;

import java.util.Set;

public class RemoveServerTimeMeasure extends TimeMeasure {
    public RemoveServerTimeMeasure() {
        super();
    }

    @Override
    protected double measure(IDistributionAlgorithm distributionAlgorithm, Set<String> topology) {
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
}
