package ru.mail.polis.service.distribution.error;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.IDistributionAlgorithm;
import ru.mail.polis.service.distribution.DistributionTest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class DistributionErrorTest<T extends IDistributionAlgorithm> extends DistributionTest<T> {
    @ParameterizedTest(name = "Distribution accuracy for {0} nodes with error less then {1}")
    @MethodSource("provideParameters")
    public void distributionAccuracyForNumberOfNodes(int numberOfNodes, double allowedError) {
        IDistributionAlgorithm distributionAlg = getAlgorithm();
        distribution(distributionAlg, numberOfNodes, allowedError);
    }

    @SuppressWarnings("unused")
    private static Stream<Arguments> provideParameters() {
        return Stream.of(
                Arguments.of(5, 0.5),
                Arguments.of(5, 0.3),
                Arguments.of(5, 0.2),
                Arguments.of(5, 0.1),
                Arguments.of(5, 0.05),

                Arguments.of(25, 0.5),
                Arguments.of(25, 0.3),
                Arguments.of(25, 0.2),
                Arguments.of(25, 0.1),
                Arguments.of(25, 0.05),

                Arguments.of(100, 0.5),
                Arguments.of(100, 0.3),
                Arguments.of(100, 0.2),
                Arguments.of(100, 0.1),
                Arguments.of(100, 0.05),

                Arguments.of(500, 0.5),
                Arguments.of(500, 0.3),
                Arguments.of(500, 0.2),
                Arguments.of(500, 0.1),
                Arguments.of(500, 0.05),

                Arguments.of(1000, 0.5),
                Arguments.of(1000, 0.3),
                Arguments.of(1000, 0.2),
                Arguments.of(1000, 0.1),
                Arguments.of(1000, 0.05)
        );
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
