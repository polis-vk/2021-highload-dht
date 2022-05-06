package ru.mail.polis.service.distribution.time;

import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.AnchorAlgorithm;

public class AnchorTimeTest extends DistributionTimeTest<AnchorAlgorithm> {
    @Override
    protected AnchorAlgorithm getAlgorithm() {
        return new AnchorAlgorithm(DEFAULT_HASH_ALGORITHM);
    }
}
