package ru.mail.polis.sharding;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public interface HashFunction {
    long hash(String key);

    class HashMD5 implements HashFunction {
        @Override
        public long hash(String key) {
            final MessageDigest instance;
            try {
                instance = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }

            instance.reset();
            instance.update(key.getBytes());
            byte[] digest = instance.digest();

            long h = 0;
            for (int i = 0; i < 4; i++) {
                h <<= 8;
                h |= ((int) digest[i]) & 0xFF;
            }
            return h;
        }
    }
}
