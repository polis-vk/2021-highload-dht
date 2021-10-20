package ru.mail.polis.lsm.artem_drozdov;

import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

class MemTable {

    private final NavigableMap<ByteBuffer, Record> internalStorage;
    private final AtomicLong size;

    public MemTable() {
        this.internalStorage = new ConcurrentSkipListMap<>();
        this.size = new AtomicLong();
    }

    public long putAndGetSize(Record record) {
        internalStorage.put(record.getKey(), record);
        return size.addAndGet(sizeOf(record));
    }

    private int sizeOf(Record record) {
        return SSTable.sizeOf(record);
    }

    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return map(fromKey, toKey).values().iterator();
    }

    private Map<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return internalStorage;
        } else if (fromKey == null) {
            return internalStorage.headMap(toKey, false);
        } else if (toKey == null) {
            return internalStorage.tailMap(fromKey, true);
        } else {
            return internalStorage.subMap(fromKey, true, toKey, false);
        }
    }
}
