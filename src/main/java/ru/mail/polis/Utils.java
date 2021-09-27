package ru.mail.polis;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class Utils {

    private Utils() {
        // Don't instantiate
    }

    public static ByteBuffer stringToBytebuffer(String text) {
        return ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
    }

    public static byte[] bytebufferToBytes(ByteBuffer buffer) {
        byte[] arr = new byte[buffer.remaining()];
        buffer.get(arr);

        return arr;
    }
}
