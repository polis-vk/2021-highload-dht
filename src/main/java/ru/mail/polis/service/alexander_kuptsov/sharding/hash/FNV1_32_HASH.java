package ru.mail.polis.service.alexander_kuptsov.sharding.hash;

// Using FNV1_32_HASH algorithm
public final class FNV1_32_HASH implements IHashAlgorithm {
    private static final int FNV1_32_INIT = 0x811c9dc5;
    private static final int FNV1_PRIME_32 = 16777619;

    public int getHash(String str) {
        int hash = FNV1_32_INIT;
        for (int i = 0; i < str.length(); i++)
            hash = (hash ^ str.charAt(i)) * FNV1_PRIME_32;
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;

        if (hash < 0)
            hash = Math.abs(hash);
        return hash;
    }
}
