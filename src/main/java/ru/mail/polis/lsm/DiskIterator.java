package ru.mail.polis.lsm;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class DiskIterator implements Iterator<Record> {
    private final ByteBuffer buffer;
    private final int toOffset;

    DiskIterator(ByteBuffer buffer, int toOffset) {
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
