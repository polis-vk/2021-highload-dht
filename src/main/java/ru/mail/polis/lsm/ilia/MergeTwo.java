package ru.mail.polis.lsm.ilia;

import ru.mail.polis.lsm.Record;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class MergeTwo implements Iterator<Record> {

    private final PeekingIterator left;
    private final PeekingIterator right;

    public MergeTwo(PeekingIterator left, PeekingIterator right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public boolean hasNext() {
        return left.hasNext() || right.hasNext();
    }

    @Override
    public Record next() {
        if (!left.hasNext() && !right.hasNext()) {
            throw new NoSuchElementException();
        }

        if (!left.hasNext()) {
            return right.next();
        }

        if (!right.hasNext()) {
            return left.next();
        }

        ByteBuffer leftKey = left.peek().getKey();
        ByteBuffer rightKey = right.peek().getKey();

        int compareResult = leftKey.compareTo(rightKey);
        if (compareResult == 0) {
            left.next();
            return right.next();
        } else if (compareResult > 0) {
            return right.next();
        } else {
            return left.next();
        }
    }
}

