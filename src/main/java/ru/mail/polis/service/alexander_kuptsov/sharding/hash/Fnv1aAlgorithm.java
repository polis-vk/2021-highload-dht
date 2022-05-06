package ru.mail.polis.service.alexander_kuptsov.sharding.hash;

public class Fnv1aAlgorithm implements IHashAlgorithm {
    private static final int INIT = 0x811c9dc5;
    private static final int PRIME = 16_777_619;

    @Override
    public int getHash(String str) {
        int hash = INIT;

        for (int i = 0; i < str.length(); i++) {
            int octect = str.charAt(i);
            hash ^= octect;
            hash *= PRIME;
        }
        return hash & 0x7fffffff;
    }
}
