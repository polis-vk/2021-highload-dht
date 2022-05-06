package ru.mail.polis.service.distribution.time;

import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.RendezvousAlgorithm;

public class RendezvousTimeTest extends DistributionTimeTest<RendezvousAlgorithm> {
    @Override
    protected RendezvousAlgorithm getAlgorithm() {
        return new RendezvousAlgorithm(DEFAULT_HASH_ALGORITHM);
    }
}