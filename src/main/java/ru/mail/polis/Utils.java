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

    public static Timestamp byteArrayToTimestamp(ByteBuffer buffer) {
//        long dateInSec = ByteBuffer.wrap(arr).getLong();
        long dateInSec = buffer.flip().getLong();

        return new Timestamp(dateInSec);
    }

    public static ByteBuffer timeStampToByteBuffer(Long dateInSec) {
//        long dateInSec = timestamp.getTime();

        return ByteBuffer.allocate(8).putLong(dateInSec);
    }
}
