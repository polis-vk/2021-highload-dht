package ru.mail.polis;

import java.nio.ByteBuffer;

public class RecordUtil {

    /**
     * Extracting byte array from ByteBuffer.
     * @param value - bytebuffer
     * @return - byte array
     */
    public static byte[] extractBytes(ByteBuffer value) {
        byte[] result = new byte[value.remaining()];
        value.get(result);
        return result;
    }

}
