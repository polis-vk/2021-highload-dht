package ru.mail.polis.service.distribution.time;

import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.MaglevAlgorithm;

public class MaglevTimeTest extends DistributionTimeTest<MaglevAlgorithm> {
    @Override
    protected MaglevAlgorithm getAlgorithm() {
        return new MaglevAlgorithm(DEFAULT_HASH_ALGORITHM);
    }
}