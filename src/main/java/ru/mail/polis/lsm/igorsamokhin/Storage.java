package ru.mail.polis.lsm.igorsamokhin;

import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

@SuppressWarnings("JdkObsolete")
@ThreadSafe
public final class Storage {
    private static final SortedMap<ByteBuffer, Record> EMPTY_STORAGE = Collections.emptySortedMap();
    public final List<SSTable> tables;
    public final SortedMap<ByteBuffer, Record> currentStorage;
    public final SortedMap<ByteBuffer, Record> storageToWrite;

    private Storage(List<SSTable> tables,
                    SortedMap<ByteBuffer, Record> currentStorage,
                    SortedMap<ByteBuffer, Record> storageToWrite) {
        this.tables = tables;
        this.currentStorage = currentStorage;
        this.storageToWrite = storageToWrite;
    }

    public static Storage init(List<SSTable> tables) {
        return new Storage(tables,
                new ConcurrentSkipListMap<>(),
                EMPTY_STORAGE);
    }

    public Storage prepareFlush() {
        return new Storage(tables, new ConcurrentSkipListMap<>(), currentStorage);
    }

    public Storage afterFlush(SSTable newTable) {
        List<SSTable> newTables = new ArrayList<>(tables.size() + 1);
        newTables.addAll(tables);
        newTables.add(newTable);
        return new Storage(newTables, currentStorage, EMPTY_STORAGE);
    }

    public Storage afterCompaction(SSTable table) {
        List<SSTable> ssTables = Collections.singletonList(table);
        return new Storage(ssTables, currentStorage, EMPTY_STORAGE);
    }

    private Iterator<Record> sstableRanges(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(tables.size());
        for (SSTable sstable : tables) {
            iterators.add(sstable.range(fromKey, toKey));
        }
        return LsmDAO.merge(iterators);
    }

    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        Iterator<Record> sstableRanges = sstableRanges(fromKey, toKey);

        Iterator<Record> currentMemoryRange = getSubMap(currentStorage, fromKey, toKey)
                .values()
                .iterator();

        Iterator<Record> flushingMemoryRange = getSubMap(storageToWrite, fromKey, toKey)
                .values()
                .iterator();

        return LsmDAO.merge(List.of(sstableRanges, flushingMemoryRange, currentMemoryRange));
    }

    /**
     * Create sub map.
     */
    public static SortedMap<ByteBuffer, Record> getSubMap(SortedMap<ByteBuffer, Record> memoryStorage,
                                                          @Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return memoryStorage;
        } else if (fromKey == null) {
            return memoryStorage.headMap(toKey);
        } else if (toKey == null) {
            return memoryStorage.tailMap(fromKey);
        }
        return memoryStorage.subMap(fromKey, toKey);
    }
}
