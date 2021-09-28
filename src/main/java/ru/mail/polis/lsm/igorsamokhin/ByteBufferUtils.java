package ru.mail.polis.lsm.igorsamokhin;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.WritableByteChannel;

final class ByteBufferUtils {
    private ByteBufferUtils() {
    }

    public static ByteBuffer readValue(MappedByteBuffer buffer) {
        if (buffer.position() == buffer.limit()) {
            return buffer.slice().limit(0).asReadOnlyBuffer();
        }

        int size = buffer.getInt();
        if (size < 0) {
            return null;
        }

        ByteBuffer value = buffer.slice().limit(size).asReadOnlyBuffer();
        buffer.position(buffer.position() + size);
        return value;
    }

    public static void writeValue(@Nullable ByteBuffer value, WritableByteChannel channel, ByteBuffer tmp)
            throws IOException {
        if (value == null) {
            writeInt(-1, channel, tmp);
            tmp.position(0);
            return;
        }
        writeInt(value.remaining(), channel, tmp);
        channel.write(value);
    }

    public static void writeInt(int value, WritableByteChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        tmp.putInt(value);
        tmp.position(0);

        channel.write(tmp);
    }
}
