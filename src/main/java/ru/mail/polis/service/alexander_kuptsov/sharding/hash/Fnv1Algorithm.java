package ru.mail.polis.service.alexander_kuptsov.sharding.hash;

public final class Fnv1Algorithm implements IHashAlgorithm {
    private static final int INIT = 0x811c9dc5;
    private static final int PRIME = 16_777_619;

    public Fnv1Algorithm() {
        super();
    }

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

        if (hash < 0) {
            hash = Math.abs(hash);
        }
        return hash;
    }
}
