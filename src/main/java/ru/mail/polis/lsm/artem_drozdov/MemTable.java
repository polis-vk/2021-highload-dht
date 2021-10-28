package ru.mail.polis.lsm.artem_drozdov;

import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class MemTable {
    private final NavigableMap<ByteBuffer, Record> memStorage;
    private final AtomicLong size;

    public MemTable() {
        this.memStorage = new ConcurrentSkipListMap<>();
        this.size = new AtomicLong();
    }

    public long put(Record record) {
        Record prev = memStorage.put(record.getKey(), record);
        return size.addAndGet(size(record) - size(prev));
    }

    private int size(@Nullable Record record) {
        return record == null ? 0 : SSTableHelper.sizeOf(record);
    }

    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return map(fromKey, toKey).values().iterator();
    }

    private Map<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return memStorage;
        } else if (fromKey == null) {
            return memStorage.headMap(toKey, false);
        } else if (toKey == null) {
            return memStorage.tailMap(fromKey, true);
        } else {
            return memStorage.subMap(fromKey, true, toKey, false);
        }
    }
}
