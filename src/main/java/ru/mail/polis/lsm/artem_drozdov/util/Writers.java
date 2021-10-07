package ru.mail.polis.lsm.artem_drozdov.util;

import ru.mail.polis.lsm.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Objects;

public final class Writers {
    private Writers() {
        // not supposed to be instantiated
    }

    public static void writeRecord(Record record, WritableByteChannel channel, ByteBuffer tmp) throws IOException {
        final boolean isTombstone = record.isTombstone();
        final int keyWriteSize = Integer.BYTES + record.getKeySize();
        final int recordWriteSize = keyWriteSize + Integer.BYTES
                + (isTombstone ? Integer.BYTES : record.getValueSize());

        if (tmp.capacity() > recordWriteSize) {
            tmp.position(0);
            putValueWithSizeToBuffer(record.getKey(), tmp);
            if (isTombstone) {
                tmp.putInt(-1);
            } else {
                ByteBuffer value = Objects.requireNonNull(record.getValue());
                putValueWithSizeToBuffer(value, tmp);
            }
            tmp.flip();
            channel.write(tmp);
            tmp.limit(tmp.capacity());
            return;
        }

        writeValueWithSize(record.getKey(), channel, tmp);
        if (isTombstone) {
            writeInt(-1, channel, tmp);
        } else {
            // value is null for tombstones only
            ByteBuffer value = Objects.requireNonNull(record.getValue());
            writeValueWithSize(value, channel, tmp);
        }
    }

    public static void putValueWithSizeToBuffer(ByteBuffer value, ByteBuffer tmp) {
        tmp.putInt(value.remaining());
        tmp.put(value);
    }

    public static void writeValueWithSize(ByteBuffer value,
                                           WritableByteChannel channel,
                                           ByteBuffer tmp) throws IOException {
        tmp.position(0);
        if (tmp.capacity() > value.remaining() + Integer.BYTES) {
            tmp.position(0);
            putValueWithSizeToBuffer(value, tmp);
            tmp.flip();
            channel.write(tmp);
            tmp.limit(tmp.capacity());
            return;
        }

        writeInt(value.remaining(), channel, tmp);
        channel.write(value);
    }

    public static void writeInt(int value, WritableByteChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        tmp.putInt(value);
        tmp.flip();

        channel.write(tmp);
        tmp.limit(tmp.capacity());
    }
}
