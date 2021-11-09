package ru.mail.polis.lsm.artemdrozdov;

import ru.mail.polis.lsm.Record;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class TombstoneFilter {

    public static Iterator<Record> filterTombstones(Iterator<Record> iterator, final boolean noTombstone) {
        PeekingIterator delegate = new PeekingIterator(iterator);
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                for (;;) {
                    Record peek = delegate.peek();
                    if (peek == null) {
                        return false;
                    }
                    if (noTombstone) {
                        if (!peek.isTombstone()) {
                            return true;
                        }
                    } else {
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
        };
    }
}
