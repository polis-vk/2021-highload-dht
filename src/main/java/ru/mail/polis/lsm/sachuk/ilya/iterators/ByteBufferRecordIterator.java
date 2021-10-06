package ru.mail.polis.lsm.sachuk.ilya.iterators;

import ru.mail.polis.lsm.Record;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public final class ByteBufferRecordIterator implements Iterator<Record> {
    private final ByteBuffer buffer;
    private final int toOffset;

    public ByteBufferRecordIterator(
            final ByteBuffer buffer,
            final int fromOffset,
            final int toOffset) {
        buffer.position(fromOffset);
        this.buffer = buffer;
        this.toOffset = toOffset;
    }

    @Override
    public boolean hasNext() {
        return buffer.position() < toOffset;
    }

    @Override
    public Record next() {
        if (!hasNext()) {
            throw new NoSuchElementException("Limit is reached");
        }

        int keySize = buffer.getInt();
        ByteBuffer key = read(keySize);

        int valueSize = buffer.getInt();
        if (valueSize == -1) {
            return Record.tombstone(key);
        }
        ByteBuffer value = read(valueSize);

        return Record.of(key, value);
    }

    private ByteBuffer read(int size) {
        ByteBuffer result = buffer.slice().limit(size);
        buffer.position(buffer.position() + size);
        return result;
    }
}
