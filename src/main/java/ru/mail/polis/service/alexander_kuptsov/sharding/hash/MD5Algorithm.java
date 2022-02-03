package ru.mail.polis.service.alexander_kuptsov.sharding.hash;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Algorithm implements IHashAlgorithm {

    public MD5Algorithm()  {
    }

    @Override
    public int getHash(String str) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        md.reset();
        md.update(str.getBytes(StandardCharsets.UTF_8));
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
