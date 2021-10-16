package ru.mail.polis.lsm.eldar_tim.components;

import ru.mail.polis.lsm.Record;

import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

abstract class AbstractMemTable implements MemTable {

    private final int id;
    protected final NavigableMap<ByteBuffer, Record> map;

    protected AbstractMemTable(int id) {
        this(id, new ConcurrentSkipListMap<>());
    }

    protected AbstractMemTable(int id, NavigableMap<ByteBuffer, Record> map) {
        this.id = id;
        this.map = map;
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public void put(Record record) {
        map.put(record.getKey(), record);
    }

    @Override
    public MemTable toReadOnly() {
        return new ReadonlyMemTable(id, map);
    }

    @Override
    public NavigableMap<ByteBuffer, Record> raw() {
        return map;
    }
}