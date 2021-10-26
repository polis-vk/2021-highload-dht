package ru.mail.polis.lsm.igorsamokhin;

import ru.mail.polis.lsm.Record;

import java.util.Iterator;

class Entry {
    public Iterator<Record> iterator;
    public Record prevRecord;
    public int order;

    /**
     * Util class to store current iterator state.
     *
     * @param iterator   the iterator
     * @param prevRecord value of the iterator
     * @param order      order of an iterator
     */
    Entry(Iterator<Record> iterator, Record prevRecord, int order) {
        this.iterator = iterator;
        this.prevRecord = prevRecord;
        this.order = order;
    }
}
