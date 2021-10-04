package ru.mail.polis.lsm.sachuk.ilya.iterators;

import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.sachuk.ilya.SSTable;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Iterator;

public class SSTableIterator implements Iterator<Record> {
    private final ByteBuffer keyToRead;
    private final boolean readToEnd;
    private final MappedByteBuffer mappedByteBuffer;
    private ByteBuffer nextKey;

    public SSTableIterator(int positionToStartRead, ByteBuffer keyToRead, MappedByteBuffer mappedByteBuffer) {
        this.keyToRead = keyToRead;

        this.readToEnd = keyToRead == null;

        this.mappedByteBuffer = mappedByteBuffer;

        if (positionToStartRead == -1) {
            mappedByteBuffer.position(mappedByteBuffer.limit());
        } else {
            mappedByteBuffer.position(positionToStartRead);
        }

        nextKey = mappedByteBuffer.hasRemaining() ? getNextKey() : null;
    }

    @Override
    public boolean hasNext() {
        if (readToEnd) {
            return mappedByteBuffer.hasRemaining();
        }

        return mappedByteBuffer.hasRemaining() && nextKey != null && nextKey.compareTo(keyToRead) < 0;
    }

    @Override
    public Record next() {
        ByteBuffer key = SSTable.readFromFile(mappedByteBuffer);
        ByteBuffer value = SSTable.readFromFile(mappedByteBuffer);

        Record record;
        if (value.compareTo(SSTable.BYTE_BUFFER_TOMBSTONE) == 0) {
            record = Record.tombstone(key);
        } else {
            value.position(0);
            record = Record.of(key, value);
        }

        nextKey = mappedByteBuffer.hasRemaining() ? getNextKey() : null;

        return record;
    }

    private ByteBuffer getNextKey() {
        int currentPos = mappedByteBuffer.position();

        ByteBuffer key = SSTable.readFromFile(mappedByteBuffer);
        mappedByteBuffer.position(currentPos);

        return key;
    }
}
