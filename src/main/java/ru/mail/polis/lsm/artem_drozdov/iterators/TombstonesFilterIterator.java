package ru.mail.polis.lsm.artem_drozdov.iterators;

import ru.mail.polis.lsm.Record;

import java.util.Iterator;
import java.util.NoSuchElementException;

public final class TombstonesFilterIterator implements Iterator<Record> {
    private final PeekingIterator delegate;

    public TombstonesFilterIterator(Iterator<Record> delegate) {
        this.delegate = new PeekingIterator(delegate);
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
