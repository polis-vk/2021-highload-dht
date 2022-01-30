package ru.mail.polis.service.distribution.time;

import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.MultiProbeAlgorithm;

public class MultiProbeTimeTest extends DistributionTimeTest<MultiProbeAlgorithm> {
    @Override
    protected MultiProbeAlgorithm getAlgorithm() {
        return new MultiProbeAlgorithm(DEFAULT_HASH_ALGORITHM);
    }
}