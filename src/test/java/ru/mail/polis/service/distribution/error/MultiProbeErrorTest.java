package ru.mail.polis.service.distribution.error;

import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.MultiProbeAlgorithm;

public class MultiProbeErrorTest extends DistributionErrorTest<MultiProbeAlgorithm> {
    @Override
    protected MultiProbeAlgorithm getAlgorithm() {
        return new MultiProbeAlgorithm(DEFAULT_HASH_ALGORITHM);
    }
}
