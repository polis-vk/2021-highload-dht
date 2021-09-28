package ru.mail.polis.lsm;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class FilterIterator implements Iterator<Record> {
    private final PeekingIterator delegate;

    FilterIterator(PeekingIterator delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
        for (;;) {
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
