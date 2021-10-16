package ru.mail.polis.lsm.eldar_tim.components;

import ru.mail.polis.lsm.Record;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public final class ReadonlyMemTable extends AbstractMemTable {

    public ReadonlyMemTable(int id) {
        this(id, new ConcurrentSkipListMap<>());
    }

    public ReadonlyMemTable(int id, NavigableMap<ByteBuffer, Record> map) {
        super(id, Collections.unmodifiableNavigableMap(map));
    }

    @Override
    public MemTable toReadOnly() {
        return this;
    }
}
