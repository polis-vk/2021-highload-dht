package ru.mail.polis.service.alexander_kuptsov.sharding.hash;

public class Fnv1Algorithm implements IHashAlgorithm {
    private static final int INIT = 0x811c9dc5;
    private static final int PRIME = 16_777_619;

    @Override
    public int getHash(String str) {
        int hash = INIT;
        for (int i = 0; i < str.length(); i++) {
            hash = (hash ^ str.charAt(i)) * PRIME;
        }
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;

        return hash & 0x7fffffff;
    }
}
