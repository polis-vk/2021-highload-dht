package ru.mail.polis.service.distribution.error;

import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.RendezvousAlgorithm;

public class RendezvousErrorTest extends DistributionErrorTest<RendezvousAlgorithm> {
    @Override
    protected RendezvousAlgorithm getAlgorithm() {
        return new RendezvousAlgorithm(DEFAULT_HASH_ALGORITHM);
    }
}
