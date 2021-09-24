package ru.mail.polis.lsm.sachuk.ilya.iterators;

import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class MergeIterator implements Iterator<Record> {

    private Record leftRecord;
    private Record rightRecord;
    private final Iterator<Record> leftIterator;
    private final Iterator<Record> rightIterator;

    public MergeIterator(Iterator<Record> leftIterator, Iterator<Record> rightIterator) {
        this.leftIterator = leftIterator;
        this.rightIterator = rightIterator;
    }

    @Override
    public boolean hasNext() {
        return leftIterator.hasNext() || rightIterator.hasNext();
    }

    @Override
    public Record next() {

        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        leftRecord = getNext(leftIterator, leftRecord);
        rightRecord = getNext(rightIterator, rightRecord);

        if (leftRecord == null) {
            return getRight();
        }
        if (rightRecord == null) {
            return getLeft();
        }

        int compare = leftRecord.getKey().compareTo(rightRecord.getKey());

        if (compare == 0) {
            leftRecord = null;
            return getRight();
        } else if (compare < 0) {
            return getLeft();
        } else {
            return getRight();
        }
    }

    private Record getRight() {
        Record tmp = rightRecord;
        rightRecord = null;

        return tmp;
    }

    private Record getLeft() {
        Record tmp = leftRecord;
        leftRecord = null;

        return tmp;
    }

    private Record getNext(Iterator<Record> iterator, @Nullable Record current) {
        if (current != null) {
            return current;
        }

        return iterator.hasNext() ? iterator.next() : null;
    }
}
