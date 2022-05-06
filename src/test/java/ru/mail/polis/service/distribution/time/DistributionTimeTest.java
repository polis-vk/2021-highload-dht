package ru.mail.polis.service.distribution.time;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.IDistributionAlgorithm;
import ru.mail.polis.service.distribution.DistributionTest;

import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class DistributionTimeTest<T extends IDistributionAlgorithm> extends DistributionTest<T> {
    @ParameterizedTest(name = "Time check for {0} nodes in topology")
    @MethodSource("provideParameters")
    public void timeForNumberOfNodes(int numberOfNodes, int targetTime) {
        IDistributionAlgorithm distributionAlg = getAlgorithm();
        timeMeasure(distributionAlg, numberOfNodes, targetTime);
    }

    @SuppressWarnings("unused")
    private static Stream<Arguments> provideParameters() {
        return Stream.of(
                Arguments.of(5, 300),
                Arguments.of(25, 350),
                Arguments.of(100, 500),
                Arguments.of(500, 750),
                Arguments.of(1000, 1000)
        );
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
