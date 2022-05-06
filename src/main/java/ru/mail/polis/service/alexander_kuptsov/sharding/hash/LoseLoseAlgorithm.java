package ru.mail.polis.service.alexander_kuptsov.sharding.hash;

public class LoseLoseAlgorithm implements IHashAlgorithm {

    @Override
    public int getHash(String str) {
        int hash = 0;
        for (int i = 0; i < str.length(); ++i) {
            hash += str.charAt(i);
        }

        return hash & 0x7fffffff;
    }
}
