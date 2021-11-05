package ru.mail.polis;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
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
        ByteBuffer buffer = ByteBuffer.allocate(8).putLong(dateInSec);
        return buffer.position(0).duplicate();
    }

    public static String toString(ByteBuffer buffer) {
        try {
            return StandardCharsets.UTF_8.newDecoder().decode(buffer).toString();
        } catch (CharacterCodingException e) {
            throw new RuntimeException(e);
        }
    }
}
