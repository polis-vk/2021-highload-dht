package ru.mail.polis.lsm.sachuk.ilya.iterators;

import ru.mail.polis.Utils;
import ru.mail.polis.lsm.Record;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public final class ByteBufferRecordIterator implements Iterator<Record> {
//    private final Logger logger = LoggerFactory.getLogger(ByteBufferRecordIterator.class);

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
            long timestampSize = buffer.getLong();
            ByteBuffer timestamp = Utils.timeStampToByteBuffer(timestampSize);

            return Record.tombstone(key, timestamp);
        }
        ByteBuffer value = read(valueSize);

        long timestampSize = buffer.getLong();
        ByteBuffer timestamp = Utils.timeStampToByteBuffer(timestampSize);
//        buffer.position(buffer.position() + timestamp.remaining());
//        int timestampSize = buffer.getInt();
//        ByteBuffer timestamp = read(timestampSize);
//        ByteBuffer timestamp = Utils.timeStampToByteBuffer(System.currentTimeMillis());
//        long timestampSize = buffer.getLong();


//        logger.info("KEY IS : " + Utils.toString(key.duplicate()) + " and value is: " + Utils.toString(value.duplicate()) + "  and timestamp is: " + Utils.byteArrayToTimestamp(timestamp.duplicate()));

        return Record.of(key, value, timestamp);
//        return Record.of(key, value, timestamp);
    }

    private ByteBuffer read(int size) {
        ByteBuffer result = buffer.slice().limit(size);
        buffer.position(buffer.position() + size);
        return result;
    }
}
