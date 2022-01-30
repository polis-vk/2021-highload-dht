package ru.mail.polis.service.alexander_kuptsov.sharding.hash;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Algorithm implements IHashAlgorithm {
    MessageDigest md;

    public MD5Algorithm() throws NoSuchAlgorithmException {
        md = MessageDigest.getInstance("MD5");
    }

    @Override
    public int getHash(String str) {
        md.reset();
        md.update(str.getBytes());
        byte[] digest = md.digest();

        int hash = 0;
        for (int i = 0; i < 4; i++) {
            hash <<= 8;
            hash |= ((int) digest[i]) & 0xFF;
        }
        if (hash < 0) {
            hash = Math.abs(hash);
        }
        return hash;
    }
}
