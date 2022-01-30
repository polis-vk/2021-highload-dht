package ru.mail.polis.service.distribution.error;

import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.VNodesAlgorithm;

public class VNodesErrorTest extends DistributionErrorTest<VNodesAlgorithm> {
    @Override
    protected VNodesAlgorithm getAlgorithm() {
        return new VNodesAlgorithm(DEFAULT_HASH_ALGORITHM, 225);
    }
}
