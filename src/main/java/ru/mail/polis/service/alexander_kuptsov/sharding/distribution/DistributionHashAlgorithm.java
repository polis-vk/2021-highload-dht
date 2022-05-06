package ru.mail.polis.service.alexander_kuptsov.sharding.distribution;

import ru.mail.polis.service.alexander_kuptsov.sharding.hash.IHashAlgorithm;

public abstract class DistributionHashAlgorithm<T extends IHashAlgorithm> implements IDistributionAlgorithm {
    private final T hashAlgorithm;

    protected DistributionHashAlgorithm(T hashAlgorithm) {
        this.hashAlgorithm = hashAlgorithm;
    }

    protected int getHash(String id) {
        return hashAlgorithm.getHash(id);
    }

    protected int getHashWithSeed(String id, int seed) {
        return hashAlgorithm.getHashWithSeed(id, seed);
    }

    protected IHashAlgorithm getHashAlgorithm() {
        return hashAlgorithm;
    }
}
