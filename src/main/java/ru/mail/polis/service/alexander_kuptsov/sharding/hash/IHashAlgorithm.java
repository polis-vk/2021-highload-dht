package ru.mail.polis.service.alexander_kuptsov.sharding.hash;

public interface IHashAlgorithm {
    int getHash(String str);

    default int getHashWithSeed(String str, int seed) {
        if (seed == 0) {
            return getHash(str);
        }
        return getHash(str.concat(String.valueOf(seed)));
    }
}
