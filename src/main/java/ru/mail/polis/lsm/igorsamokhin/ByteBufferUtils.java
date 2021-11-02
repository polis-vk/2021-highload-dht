package ru.mail.polis.lsm.igorsamokhin;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
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

    public static void putInBufferOrWriteWithSize(FileChannel channel,
                                                  ByteBuffer tmp,
                                                  @Nullable ByteBuffer... buffers) throws IOException {
        if (buffers == null) {
            return;
        }

        for (ByteBuffer buffer : buffers) {
            int sizeToWrite = (buffer == null) ? Integer.BYTES : (Integer.BYTES + buffer.remaining());
            if (tmp.remaining() < sizeToWrite) {
                writeByteBuffer(channel, tmp);

                if ((tmp.limit() < sizeToWrite) && (buffer != null)) {
                    writeInt(buffer.remaining(), channel, tmp);
                    channel.write(buffer);
                    continue;
                }
            }

            if (buffer == null) {
                tmp.putInt(-1);
            } else {
                tmp.putInt(buffer.remaining());
                tmp.put(buffer);
            }
        }

        if (tmp.position() != 0) {
            writeByteBuffer(channel, tmp);
        }
    }

    public static void writeByteBuffer(WritableByteChannel channel, ByteBuffer tmp) throws IOException {
        int limit = tmp.limit();
        tmp.flip();
        channel.write(tmp);
        tmp.limit(limit);
        tmp.position(0);
    }

    public static void writeInt(int value, WritableByteChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        tmp.putInt(value);
        writeByteBuffer(channel, tmp);
    }

    public static void putLongInBufferOrWrite(long value, FileChannel channel, ByteBuffer tmp) throws IOException {
        if (tmp.remaining() < Long.BYTES) {
            writeByteBuffer(channel, tmp);
        }

        tmp.putLong(value);
    }
}
