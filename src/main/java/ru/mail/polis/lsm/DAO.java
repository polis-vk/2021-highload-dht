package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Minimal database API.
 */
public interface DAO extends Closeable {
    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey);

    /**
     * Update/insert.
     *
     * @param record record for updating/inserting/deleting
     * @throws UncheckedIOException throws if DAO closed
     */
    void upsert(Record record);

    void compact();

    /**
     * Appends {@code Byte.MIN_VALUE} to {@code buffer}.
     *
     * @param buffer original {@link ByteBuffer}
     * @return copy of {@code buffer} with {@code Byte.MIN_VALUE} appended
     */
    static ByteBuffer nextKey(ByteBuffer buffer) {
        ByteBuffer result = ByteBuffer.allocate(buffer.remaining() + 1);

        int position = buffer.position();

        result.put(buffer);
        result.put(Byte.MIN_VALUE);

        buffer.position(position);
        result.rewind();

        return result;
    }
}
