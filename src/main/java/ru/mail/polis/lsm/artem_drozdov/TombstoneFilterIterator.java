package ru.mail.polis.lsm.artem_drozdov;

import ru.mail.polis.lsm.Record;

import java.util.Iterator;

class TombstoneFilterIterator implements Iterator<Record> {
    private final PeekingIterator delegate;

    public TombstoneFilterIterator(PeekingIterator delegate) {
        this.delegate = delegate;
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
        return delegate.next();
    }
}
