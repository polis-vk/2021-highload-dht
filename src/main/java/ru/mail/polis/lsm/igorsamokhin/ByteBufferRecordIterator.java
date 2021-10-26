package ru.mail.polis.lsm.igorsamokhin;

import ru.mail.polis.lsm.Record;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

final class ByteBufferRecordIterator implements Iterator<Record> {
    private final LongMappedByteBuffer buffer;
    private final long toOffset;

    ByteBufferRecordIterator(
            final LongMappedByteBuffer buffer,
            final long fromOffset,
            final long toOffset) {
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
        ByteBuffer result = buffer.cut(size);
        buffer.position(buffer.position() + size);
        return result;
    }
}
