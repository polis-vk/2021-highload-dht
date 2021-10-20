package ru.mail.polis.lsm.igorsamokhin;

import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("JdkObsolete")
public class MemTable {

    private final SortedMap<ByteBuffer, Record> internalStorage;
    private final AtomicInteger size;

    public MemTable() {
        this.internalStorage = new ConcurrentSkipListMap<>();
        this.size = new AtomicInteger(0);
    }

    public int putAndGetSize(Record record) {
        Record prev = internalStorage.put(record.getKey(), record);
        int prevSize = (prev == null) ? 0 : prev.size();
        return size.addAndGet(record.size() - prevSize);
    }

    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return getSubMap(internalStorage, fromKey, toKey).values().iterator();
    }

    /**
     * Create sub map.
     */
    public static Map<ByteBuffer, Record> getSubMap(SortedMap<ByteBuffer, Record> memoryStorage,
                                                    @Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return memoryStorage;
        }
        if (fromKey == null) {
            return memoryStorage.headMap(toKey);
        }
        if (toKey == null) {
            return memoryStorage.tailMap(fromKey);
        }
        return memoryStorage.subMap(fromKey, toKey);
    }
}
