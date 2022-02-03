package ru.mail.polis.service.distribution.error;

import org.junit.jupiter.api.Test;
import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.IDistributionAlgorithm;
import ru.mail.polis.service.distribution.DistributionTest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class DistributionErrorTest<T extends IDistributionAlgorithm> extends DistributionTest<T> {
    /**
     * 5 nodes
     */
    @Test
    public void distributionFor5Nodes50percent() {
        IDistributionAlgorithm distributionAlg = getAlgorithm();
        distribution(distributionAlg, 5, 0.5);
    }

    @Test
    public void distributionFor5Nodes30percent() {
        IDistributionAlgorithm distributionAlg = getAlgorithm();
        distribution(distributionAlg, 5, 0.3);
    }

    @Test
    public void distributionFor5Nodes20percent() {
        IDistributionAlgorithm distributionAlg = getAlgorithm();
        distribution(distributionAlg, 5, 0.2);
    }

    @Test
    public void distributionFor5Nodes10percent() {
        IDistributionAlgorithm distributionAlg = getAlgorithm();
        distribution(distributionAlg, 5, 0.1);
    }

    @Test
    public void distributionFor5Nodes05percent() {
        IDistributionAlgorithm distributionAlg = getAlgorithm();
        distribution(distributionAlg, 5, 0.05);
    }

    /**
     * 25 nodes
     */
    @Test
    public void distributionFor25Nodes50percent() {
        IDistributionAlgorithm distributionAlg = getAlgorithm();
        distribution(distributionAlg, 25, 0.5);
    }

    @Test
    public void distributionFor25Nodes30percent() {
        IDistributionAlgorithm distributionAlg = getAlgorithm();
        distribution(distributionAlg, 25, 0.3);
    }

    @Test
    public void distributionFor25Nodes20percent() {
        IDistributionAlgorithm distributionAlg = getAlgorithm();
        distribution(distributionAlg, 25, 0.2);
    }

    @Test
    public void distributionFor25Nodes10percent() {
        IDistributionAlgorithm distributionAlg = getAlgorithm();
        distribution(distributionAlg, 25, 0.1);
    }

    @Test
    public void distributionFor25Nodes05percent() {
        IDistributionAlgorithm distributionAlg = getAlgorithm();
        distribution(distributionAlg, 25, 0.05);
    }

    /**
     * 100 nodes
     */
    @Test
    public void distributionFor100Nodes50percent() {
        IDistributionAlgorithm distributionAlg = getAlgorithm();
        distribution(distributionAlg, 100, 0.5);
    }

    @Test
    public void distributionFor100Nodes30percent() {
        IDistributionAlgorithm distributionAlg = getAlgorithm();
        distribution(distributionAlg, 100, 0.3);
    }

    @Test
    public void distributionFor100Nodes20percent() {
        IDistributionAlgorithm distributionAlg = getAlgorithm();
        distribution(distributionAlg, 100, 0.2);
    }

    @Test
    public void distributionFor100Nodes10percent() {
        IDistributionAlgorithm distributionAlg = getAlgorithm();
        distribution(distributionAlg, 100, 0.1);
    }

    @Test
    public void distributionFor100Nodes05percent() {
        IDistributionAlgorithm distributionAlg = getAlgorithm();
        distribution(distributionAlg, 100, 0.05);
    }

    /**
     * 500 nodes
     */
    @Test
    public void distributionFor500Nodes50percent() {
        IDistributionAlgorithm distributionAlg = getAlgorithm();
        distribution(distributionAlg, 500, 0.5);
    }

    @Test
    public void distributionFor500Nodes30percent() {
        IDistributionAlgorithm distributionAlg = getAlgorithm();
        distribution(distributionAlg, 500, 0.3);
    }

    @Test
    public void distributionFor500Nodes20percent() {
        IDistributionAlgorithm distributionAlg = getAlgorithm();
        distribution(distributionAlg, 500, 0.2);
    }

    @Test
    public void distributionFor500Nodes10percent() {
        IDistributionAlgorithm distributionAlg = getAlgorithm();
        distribution(distributionAlg, 500, 0.1);
    }

    @Test
    public void distributionFor500Nodes05percent() {
        IDistributionAlgorithm distributionAlg = getAlgorithm();
        distribution(distributionAlg, 500, 0.05);
    }

    private void distribution(IDistributionAlgorithm distributionAlgorithm, int numberOfNodes, double allowedError) {
        Set<String> topology = getRandomNodes(numberOfNodes);
        distributionAlgorithm.addTopology(topology);
        Map<String, Integer> numberOfEachNode = new HashMap<>();
        for (int j = 0; j < COUNT_OF_KEYS; j++) {
            final String node = distributionAlgorithm.getServer(randomId());
            numberOfEachNode.merge(node, 1, Integer::sum);
        }
        int numberOfAllNodes = 0;
        for (final Map.Entry<String, Integer> node : numberOfEachNode.entrySet()) {
            numberOfAllNodes += node.getValue();
        }
        final int average = numberOfAllNodes / topology.size();
        final double distributionError = calculateDistributionError(average, numberOfEachNode);
        assertTrue(distributionError < allowedError);
    }

    private static double calculateDistributionError(final int average, final Map<String, Integer> numberOfEachNode) {
        ArrayList<Integer> errorList = new ArrayList<>();
        for (final Map.Entry<String, Integer> node : numberOfEachNode.entrySet()) {
            final int numberOfNodes = node.getValue();
            errorList.add(Math.abs(average - numberOfNodes));
        }
        return errorList
                .stream()
                .mapToDouble(d -> d)
                .average()
                .orElse(0.0) / average;

    }
}
