package ru.mail.polis.service.alexander_kuptsov.sharding.hash;

public class Djb2Algorithm implements IHashAlgorithm {

    @Override
    public int getHash(String str) {
        int hash = 5381;
        for (int i = 0; i < str.length(); ++i) {
            hash = ((hash << 5) + hash) + str.charAt(i);
        }

        return hash & 0x7fffffff;
    }
}
