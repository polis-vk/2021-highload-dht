package ru.mail.polis;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;

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

    public static Timestamp byteBufferToTimestamp(ByteBuffer buffer) {
        long dateInSec = buffer.position(0).getLong();

        return new Timestamp(dateInSec);
    }

    public static ByteBuffer timeStampToByteBuffer(Long dateInSec) {
        ByteBuffer buffer = ByteBuffer.allocate(8).putLong(dateInSec);
        return buffer.position(0).duplicate();
    }

    public static ByteBuffer wrap(String text) {
        return ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
    }

}
