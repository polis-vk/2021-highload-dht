package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class DiskIterator implements Iterator<Record> {
    private final ByteBuffer fromKey;
    private final ByteBuffer toKey;
    private Record current;
    private final ByteBuffer buffer;
    private final ByteBuffer idx;
    private final boolean isDirectOrder;
    private boolean bufferIsOver;

    /**
     * Iterator from hard drive.
     */
    public DiskIterator(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey, ByteBuffer buffer, ByteBuffer index,
                        Boolean isDirectOrder) {
        this.fromKey = fromKey;
        this.toKey = toKey;
        this.buffer = buffer;
        this.idx = index;
        this.isDirectOrder = isDirectOrder;
        moveToStart();
        loadCurrent();
    }

    @Override
    public boolean hasNext() {
        return current != null;
    }

    @Override
    public Record next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Record result = current;
        loadCurrent();
        return result;
    }

    private void loadCurrent() {
        if (isDirectOrder) {
            loadNextRecord();
        } else {
            loadPreviousRecord();
        }
    }

    private void loadPreviousRecord() {
        if (bufferIsOver) {
            current = null;
            return;
        }
        int keySize = buffer.getInt();
        ByteBuffer key = buffer.slice().limit(keySize).asReadOnlyBuffer();

        buffer.position(buffer.position() + keySize);

        int valueSize = buffer.getInt();
        if (valueSize < 0) {
            key.position(0);
            current = Record.tombstone(key);
        } else {
            ByteBuffer value = buffer.slice().limit(valueSize).asReadOnlyBuffer();
            current = Record.of(key, value);
        }

        int pos = getPreviousRecordPosition();
        if (pos < 0) {
            bufferIsOver = true;
        } else {
            buffer.position(pos);
        }
    }

    private void loadNextRecord() {
        if (!buffer.hasRemaining() || (current != null && toKey != null && current.getKey().compareTo(toKey) >= 0)) {
            current = null;
            return;
        }
        int keySize = buffer.getInt();
        ByteBuffer key = buffer.slice().limit(keySize).asReadOnlyBuffer();

        buffer.position(buffer.position() + keySize);

        int valueSize = buffer.getInt();
        if (valueSize < 0) {
            key.position(0);
            current = Record.tombstone(key);

        } else {
            ByteBuffer value = buffer.slice().limit(valueSize).asReadOnlyBuffer();

            buffer.position(buffer.position() + valueSize);

            current = Record.of(key, value);
        }
    }

    private void moveToStart() {
        if (isDirectOrder) {
            moveToFirst();
        } else {
            moveToLast();
        }
    }

    private void moveToFirst() {
        if (fromKey == null) {
            buffer.position(0);
            return;
        }
        ByteBuffer buf = buffer.asReadOnlyBuffer();
        int maxSize = buffer.remaining();
        int fromOffset = offset(buf, fromKey);
        buffer.position(fromOffset == -1 ? maxSize : fromOffset);
    }

    private void moveToLast() {
        if (toKey == null) {
            idx.position(idx.remaining());
            int pos = getPreviousRecordPosition();
            buffer.position(Math.max(pos, 0));
            return;
        }
        ByteBuffer buf = buffer.asReadOnlyBuffer();
        int maxSize = buffer.remaining();
        int toOffset = offset(buf, toKey);
        buffer.position(toOffset == -1 ? maxSize : toOffset);
    }

    private int getPreviousRecordPosition() {
        int right = idx.position() - Integer.BYTES;
        if (right >= 0) {
            int pos = idx.getInt(right);
            idx.position(right);
            return pos;
        } else {
            return -1;
        }
    }

    private int offset(ByteBuffer buffer, ByteBuffer key) {
        int left = 0;
        int rightLimit = idx.remaining() / Integer.BYTES;
        int right = rightLimit;

        while (left < right) {
            int mid = left + ((right - left) >>> 1);

            int offset = idx.getInt(mid * Integer.BYTES);
            buffer.position(offset);
            int keySize = buffer.getInt();

            int result;
            int mismatch = buffer.mismatch(key);
            if (mismatch == -1) {
                return offset;
            } else if (mismatch < keySize) {
                result = Byte.compare(
                        key.get(key.position() + mismatch),
                        buffer.get(buffer.position() + mismatch)
                );
            } else {
                result = -1;
            }

            if (result > 0) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        if (left >= rightLimit) {
            return -1;
        }
        int pos = left * Integer.BYTES;

        if (!isDirectOrder) {
            pos = Math.max(pos - Integer.BYTES, 0);
        }
        idx.position(pos);

        return idx.getInt(pos);
    }
}
