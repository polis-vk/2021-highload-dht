package ru.mail.polis.service.distribution.time;

import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.JumpAlgorithm;

public class JumpTimeTest extends DistributionTimeTest<JumpAlgorithm> {
    @Override
    protected JumpAlgorithm getAlgorithm() {
        return new JumpAlgorithm(DEFAULT_HASH_ALGORITHM);
    }
}