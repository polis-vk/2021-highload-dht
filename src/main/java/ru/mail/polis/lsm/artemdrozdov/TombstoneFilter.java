package ru.mail.polis.lsm.artemdrozdov;

import ru.mail.polis.lsm.Record;

import java.util.Iterator;
import java.util.NoSuchElementException;

final public class TombstoneFilter {

    private TombstoneFilter() {

    }

    public static Iterator<Record> filterTombstones(Iterator<Record> iterator, final boolean noTombstone) {
        PeekingIterator delegate = new PeekingIterator(iterator);
        return getIterator(delegate, noTombstone);
    }

    private static Iterator getIterator(PeekingIterator delegate, final boolean noTombstone) {

        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return checkNext(delegate, noTombstone);
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

    private static boolean checkNext(final PeekingIterator delegate, final boolean noTombstone) {
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
}
