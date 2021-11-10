package ru.mail.polis;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
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
//        long dateInSec = ByteBuffer.wrap(arr).getLong();
        long dateInSec = buffer.position(0).getLong();

        return new Timestamp(dateInSec);
    }

    public static ByteBuffer timeStampToByteBuffer(Long dateInSec) {
//        long dateInSec = timestamp.getTime();
        ByteBuffer buffer = ByteBuffer.allocate(8).putLong(dateInSec);
        return buffer.position(0).duplicate();
    }

//    public static String toString(ByteBuffer buffer) {
//        return new String(buffer.asReadOnlyBuffer().array(), StandardCharsets.UTF_8);
//    }

//    public static String toString(ByteBuffer buffer) {
//        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
//        try {
//            return decoder.decode(buffer.position(0).duplicate()) + "";
//        } catch (CharacterCodingException e) {
//            throw new RuntimeException(e);
//        }
//    }

//    public static String toString(CharsetDecoder decoder, ByteBuffer key) {
//        try {
//            return decoder.decode(key.duplicate()).toString();
//        } catch (CharacterCodingException e) {
//            throw new RuntimeException(e);
//        }
//    }

    public static ByteBuffer wrap(String text) {
        return ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
    }

}
