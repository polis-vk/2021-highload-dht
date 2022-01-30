package ru.mail.polis.service.distribution.error;

import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.AnchorAlgorithm;

public class AnchorErrorTest extends DistributionErrorTest<AnchorAlgorithm> {
    @Override
    protected AnchorAlgorithm getAlgorithm() {
        return new AnchorAlgorithm(DEFAULT_HASH_ALGORITHM);
    }
}

