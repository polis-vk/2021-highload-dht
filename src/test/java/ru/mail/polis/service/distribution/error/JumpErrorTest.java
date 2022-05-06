package ru.mail.polis.service.distribution.error;

import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.JumpAlgorithm;

public class JumpErrorTest extends DistributionErrorTest<JumpAlgorithm> {
    @Override
    protected JumpAlgorithm getAlgorithm() {
        return new JumpAlgorithm(DEFAULT_HASH_ALGORITHM);
    }
}
