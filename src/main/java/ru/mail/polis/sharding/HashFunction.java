package ru.mail.polis.sharding;

import net.openhft.hashing.LongHashFunction;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public interface HashFunction {
    long hash(@Nonnull String key);

    class HashXXH3 implements HashFunction {
        private final LongHashFunction instance;

        public HashXXH3() {
            instance = LongHashFunction.xx3();
        }

        @Override
        public long hash(@Nonnull String key) {
            return instance.hashChars(key);
        }
    }

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
        public synchronized long hash(@Nonnull String key) {
            instance.reset();
            instance.update(key.getBytes(StandardCharsets.US_ASCII));
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
