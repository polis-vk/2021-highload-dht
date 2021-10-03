package ru.mail.polis.lsm.alex;

import ru.mail.polis.lsm.Record;

import java.util.Iterator;

public class FlushTable {

    private final int index;
    private final Iterator<Record> records;

    public FlushTable(int index, Iterator<Record> records) {
        this.index = index;
        this.records = records;
    }

    public int getIndex() {
        return index;
    }

    public Iterator<Record> getRecords() {
        return records;
    }
}
