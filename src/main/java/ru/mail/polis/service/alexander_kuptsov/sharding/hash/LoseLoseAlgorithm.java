package ru.mail.polis.service.alexander_kuptsov.sharding.hash;

public class LoseLoseAlgorithm implements IHashAlgorithm {
    public LoseLoseAlgorithm() {
        // Default constructor
    }

    @Override
    public int getHash(String str) {
        int hash = 0;
        for (int i = 0; i < str.length(); ++i) {
            hash += str.charAt(i);
        }
        if (hash < 0) {
            hash = Math.abs(hash);
        }
        return hash;
    }
}
