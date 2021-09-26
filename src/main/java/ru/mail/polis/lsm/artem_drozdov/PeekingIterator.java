package ru.mail.polis.lsm.artem_drozdov;

import ru.mail.polis.lsm.Record;

import java.util.Iterator;
import java.util.NoSuchElementException;

class PeekingIterator implements Iterator<Record> {

    private Record current;

    private final Iterator<Record> delegate;

    public PeekingIterator(Iterator<Record> delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
        return current != null || delegate.hasNext();
    }

    @Override
    public Record next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Record now = peek();
        current = null;
        return now;
    }

    public Record peek() {
        if (current != null) {
            return current;
        }

        if (!delegate.hasNext()) {
            return null;
        }

        current = delegate.next();
        return current;
    }

}