package ru.mail.polis.service.distribution.error;

import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.MaglevAlgorithm;

public class MaglevErrorTest extends DistributionErrorTest<MaglevAlgorithm> {
    @Override
    protected MaglevAlgorithm getAlgorithm() {
        return new MaglevAlgorithm(DEFAULT_HASH_ALGORITHM);
    }
}