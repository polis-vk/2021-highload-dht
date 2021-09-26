package ru.mail.polis.lsm.artemdrozdov;

import ru.mail.polis.lsm.Record;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

public class MergeTwoIterator implements Iterator<Record> {
    final private PeekingIterator left;
    final private PeekingIterator right;

    MergeTwoIterator(PeekingIterator left, PeekingIterator right) {
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

        return (compareResult < 0) ? left.next() : right.next();
    }

}
