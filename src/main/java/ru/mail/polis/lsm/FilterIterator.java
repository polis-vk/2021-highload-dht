package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class FilterIterator implements Iterator<Record> {
    private final Iterator<Record> iter;
    private Record current;
    private final ByteBuffer toKey;
    private final boolean isDirectOrder;

    /**
     * Iterator filtering tombstones and toKey.
     */
    public FilterIterator(Iterator<Record> iterator, @Nullable ByteBuffer toKey, boolean isDirectOrder) {
        this.iter = iterator;
        this.toKey = toKey;
        this.isDirectOrder = isDirectOrder;
        getCurrent();
    }

    @Override
    public boolean hasNext() {
        if (current == null) {
            return false;
        }

        if (toKey == null) {
            return true;
        }
        if (isDirectOrder) {
            return current.getKey().compareTo(toKey) < 0;
        } else {
            return current.getKey().compareTo(toKey) >= 0;
        }
    }

    @Override
    public Record next() {
        if (!hasNext()) throw new NoSuchElementException();
        Record result = current;
        getCurrent();
        return result;
    }

    private void getCurrent() {
        Record next = Record.tombstone(ByteBuffer.allocate(Integer.BYTES));
        while (next.isTombstone() && iter.hasNext()) {
            next = iter.next();
        }
        if (next.isTombstone()) {
            current = null;
        } else {
            current = next;
        }
    }
}
