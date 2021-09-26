package ru.mail.polis.lsm.alexnifontov;

import java.util.Iterator;
import java.util.NoSuchElementException;

public final class PeekingIterator<T> implements Iterator<T> {

    private final Iterator<T> delegate;
    private T current;

    public PeekingIterator(Iterator<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
        return current != null || delegate.hasNext();
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        T now = peek();
        current = null;
        return now;
    }

    public T peek() {
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
