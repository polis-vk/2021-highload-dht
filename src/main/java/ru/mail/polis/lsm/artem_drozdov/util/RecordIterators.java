package ru.mail.polis.lsm.artem_drozdov.util;

import ru.mail.polis.lsm.Record;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

public final class RecordIterators {
    private RecordIterators() {
        // Not supposed to be instantiated
    }

    public static Iterator<Record> range(ByteBuffer buffer, int fromOffset, int toOffset) {
        buffer.position(fromOffset);

        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return buffer.position() < toOffset;
            }

            @Override
            public Record next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("Limit is reached");
                }

                int keySize = buffer.getInt();
                ByteBuffer key = read(keySize);

                int valueSize = buffer.getInt();
                if (valueSize == -1) {
                    return Record.tombstone(key);
                }
                ByteBuffer value = read(valueSize);

                return Record.of(key, value);
            }

            private ByteBuffer read(int size) {
                ByteBuffer result = buffer.slice().limit(size);
                buffer.position(buffer.position() + size);
                return result;
            }
        };
    }

    public static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        if (iterators.isEmpty()) {
            return Collections.emptyIterator();
        }
        if (iterators.size() == 1) {
            return iterators.get(0);
        }
        if (iterators.size() == 2) {
            return new MergingIterator(new PeekingIterator(iterators.get(0)), new PeekingIterator(iterators.get(1)));
        }
        Iterator<Record> left = merge(iterators.subList(0, iterators.size() / 2));
        Iterator<Record> right = merge(iterators.subList(iterators.size() / 2, iterators.size()));
        return new MergingIterator(new PeekingIterator(left), new PeekingIterator(right));
    }

    public static Iterator<Record> filterTombstones(Iterator<Record> delegate) {
        return new Iterator<>() {
            private Record nextRecord = nextNotTombstone();

            @Override
            public boolean hasNext() {
                return nextRecord != null;
            }

            @Override
            public Record next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("No elements");
                }
                Record recordToReturn = this.nextRecord;
                this.nextRecord = nextNotTombstone();
                return recordToReturn;
            }

            private Record nextNotTombstone() {
                Record next = null;
                while (next == null && delegate.hasNext()) {
                    next = delegate.next();
                    if (next.isTombstone()) {
                        next = null;
                    }
                }
                return next;
            }
        };
    }

    public static class MergingIterator implements Iterator<Record> {
        private final PeekingIterator left;
        private final PeekingIterator right;

        public MergingIterator(PeekingIterator left, PeekingIterator right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public boolean hasNext() {
            return left.hasNext() || right.hasNext();
        }

        @Override
        public Record next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No elements");
            }

            if (!left.hasNext()) {
                return right.next();
            }
            if (!right.hasNext()) {
                return left.next();
            }

            // checked earlier
            ByteBuffer leftKey = Objects.requireNonNull(left.peek()).getKey();
            ByteBuffer rightKey = Objects.requireNonNull(right.peek()).getKey();

            int compareResult = leftKey.compareTo(rightKey);
            if (compareResult == 0) {
                left.next();
                return right.next();
            }

            if (compareResult < 0) {
                return left.next();
            } else {
                return right.next();
            }
        }
    }

    public static class PeekingIterator implements Iterator<Record> {

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
}
