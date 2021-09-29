package ru.mail.polis.lsm.artem_drozdov;

import ru.mail.polis.lsm.Record;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class FilterTombstonesIterator implements Iterator<Record> {
    private final PeekingIterator delegate;

    FilterTombstonesIterator(final Iterator<Record> delegate) {
        this.delegate = new PeekingIterator(delegate);
    }

    @Override
    public boolean hasNext() {
        for (; ; ) {
            Record peek = delegate.peek();
            if (peek == null) {
                return false;
            }
            if (!peek.isTombstone()) {
                return true;
            }

            delegate.next();
        }
    }

    @Override
    public Record next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No elements");
        }
        return delegate.next();
    }
}
