package ru.mail.polis.sharding;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public interface HashFunction {
    long hash(String key);

    class HashMD5 implements HashFunction {
        private final MessageDigest instance;

        public HashMD5() {
            try {
                instance = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public synchronized long hash(String key) {
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
