package ru.mail.polis.service.distribution.time;

import org.junit.jupiter.api.Test;
import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.IDistributionAlgorithm;
import ru.mail.polis.service.distribution.DistributionTest;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class DistributionTimeTest<T extends IDistributionAlgorithm> extends DistributionTest<T> {
    @Test
    public void timeFor5Nodes() {
        IDistributionAlgorithm distributionAlg = getAlgorithm();
        timeMeasure(distributionAlg, 5, 300);
    }

    @Test
    public void timeFor25Nodes() {
        IDistributionAlgorithm distributionAlg = getAlgorithm();
        timeMeasure(distributionAlg, 25, 350);
    }

    @Test
    public void timeFor100Nodes() {
        IDistributionAlgorithm distributionAlg = getAlgorithm();
        timeMeasure(distributionAlg, 100, 500);
    }

    @Test
    public void timeFor500Nodes() {
        IDistributionAlgorithm distributionAlg = getAlgorithm();
        timeMeasure(distributionAlg, 500, 750);
    }

    @Test
    public void timeFor1000Nodes() {
        IDistributionAlgorithm distributionAlg = getAlgorithm();
        timeMeasure(distributionAlg, 1000, 1000);
    }

    private void timeMeasure(IDistributionAlgorithm distributionAlgorithm, int numberOfNodes, long targetTime) {
        Set<String> topology = getRandomNodes(numberOfNodes);
        distributionAlgorithm.addTopology(topology);
        long diff = 0;
        for (int j = 0; j < COUNT_OF_KEYS; j++) {
            long startTime = System.nanoTime();
            distributionAlgorithm.getServer(randomId());
            long stopTime = System.nanoTime();
            diff = (diff + (stopTime - startTime)) / 2;
        }
        assertTrue(diff < targetTime);
    }
}
