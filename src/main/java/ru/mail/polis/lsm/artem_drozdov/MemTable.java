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

    private final NavigableMap<ByteBuffer, Record> internalStorage = new ConcurrentSkipListMap<>();
    private final AtomicLong size = new AtomicLong();

    public MemTable() {
    }

    public long putAndGetSize(Record record) {
        Record previous = internalStorage.put(record.getKey(), record);
        return size.addAndGet(sizeOf(record) - sizeOf(previous));
    }

    private int sizeOf(@Nullable Record record) {
        if (record == null) {
            return 0;
        }
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
