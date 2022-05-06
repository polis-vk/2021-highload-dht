package ru.mail.polis.service.distribution.time;

import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.VNodesAlgorithm;

public class VNodesTimeTest extends DistributionTimeTest<VNodesAlgorithm> {
    @Override
    protected VNodesAlgorithm getAlgorithm() {
        return new VNodesAlgorithm(DEFAULT_HASH_ALGORITHM, 225);
    }
}