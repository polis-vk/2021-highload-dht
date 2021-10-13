package ru.mail.polis.lsm.artem_drozdov;

import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.artem_drozdov.util.RecordIterators;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Storage {
    public final static class MemoryWithSize {
        private final int memorySize;
        private final NavigableMap<ByteBuffer, Record> memory;

        private MemoryWithSize(int memorySize, NavigableMap<ByteBuffer, Record> memory) {
            this.memorySize = memorySize;
            this.memory = memory;
        }

        public int getMemorySize() {
            return memorySize;
        }

        public NavigableMap<ByteBuffer, Record> getMemory() {
            return memory;
        }
    }

    private final AtomicInteger currentStorageMemoryUsage;

    public NavigableMap<Integer, MemoryWithSize> getFlushingStorages() {
        return flushingStorages;
    }

    private final NavigableMap<Integer, MemoryWithSize> flushingStorages;
    private final NavigableMap<ByteBuffer, Record> currentStorage;
    private final List<SSTable> tables;

    public Storage() {
        currentStorageMemoryUsage = new AtomicInteger(0);
        this.flushingStorages = new TreeMap<>();
        this.currentStorage = new ConcurrentSkipListMap<>();
        this.tables = new ArrayList<>();
    }

    public Storage(List<SSTable> tables) {
        currentStorageMemoryUsage = new AtomicInteger(0);
        this.flushingStorages = new TreeMap<>();
        this.currentStorage = new ConcurrentSkipListMap<>();
        this.tables = tables;
    }

    private Storage(NavigableMap<ByteBuffer, Record> currentStorage, int currentStorageMemoryUsage,
                    NavigableMap<Integer, MemoryWithSize> flushingStorages, List<SSTable> tables) {
        this.currentStorage = currentStorage;
        this.currentStorageMemoryUsage = new AtomicInteger(currentStorageMemoryUsage);
        this.flushingStorages = flushingStorages;
        this.tables = tables;
    }

    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        Iterator<Record> sstableRanges = sstableRanges(fromKey, toKey);
        Iterator<Record> memoryRange = map(fromKey, toKey);
        Iterator<Record> iterator = new RecordIterators.MergingIterator(
                new RecordIterators.PeekingIterator(sstableRanges),
                new RecordIterators.PeekingIterator(memoryRange)
        );
        return RecordIterators.filterTombstones(iterator);
    }

    public Storage willStartFlush(int index) {
        final NavigableMap<Integer, MemoryWithSize> newFlushingStorages = new TreeMap<>(flushingStorages);
        newFlushingStorages.put(index, new MemoryWithSize(currentStorageMemoryUsage.get(), currentStorage));
        return new Storage(new ConcurrentSkipListMap<>(), 0, newFlushingStorages, tables);
    }

    public Storage doneFlush(int index, SSTable table) {
        final NavigableMap<Integer, MemoryWithSize> newFlushingStorages = new TreeMap<>(flushingStorages);
        newFlushingStorages.remove(index);
        final List<SSTable> newTables = new ArrayList<>(tables.size() + 1);
        newTables.addAll(tables);
        newTables.add(table);
        return new Storage(currentStorage, currentStorageMemoryUsage.get(), newFlushingStorages, newTables);
    }

    public Storage restoreMemory(int index) {
        MemoryWithSize previousStorage = flushingStorages.get(index);
        currentStorage.putAll(previousStorage.getMemory());
        return new Storage(currentStorage, currentStorageMemoryUsage.addAndGet(previousStorage.getMemorySize()),
                flushingStorages, tables);
    }

    private Iterator<Record> sstableRanges(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(tables.size());
        for (SSTable ssTable : tables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return RecordIterators.merge(iterators);
    }

    private Iterator<Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return new RecordIterators.MergingIterator(
                    new RecordIterators.PeekingIterator(currentStorage.values().iterator()),
                    new RecordIterators.PeekingIterator(
                            RecordIterators.merge(flushingStorages.values().stream()
                                    .map(e -> e.memory.values().iterator())
                                    .collect(Collectors.toList()))
                    )
            );
        }
        if (fromKey == null) {
            return new RecordIterators.MergingIterator(
                    new RecordIterators.PeekingIterator(currentStorage.headMap(toKey).values().iterator()),
                    new RecordIterators.PeekingIterator(
                            RecordIterators.merge(flushingStorages.values().stream()
                                    .map(e -> e.memory.headMap(toKey).values().iterator())
                                    .collect(Collectors.toList()))
                    )
            );
        }
        if (toKey == null) {
            return new RecordIterators.MergingIterator(
                    new RecordIterators.PeekingIterator(currentStorage.tailMap(fromKey).values().iterator()),
                    new RecordIterators.PeekingIterator(
                            RecordIterators.merge(flushingStorages.values().stream()
                                    .map(e -> e.memory.tailMap(fromKey).values().iterator())
                                    .collect(Collectors.toList()))
                    )
            );
        }
        return new RecordIterators.MergingIterator(
                new RecordIterators.PeekingIterator(currentStorage.subMap(fromKey, toKey).values().iterator()),
                new RecordIterators.PeekingIterator(
                        RecordIterators.merge(flushingStorages.values().stream()
                                .map(e -> e.memory.subMap(fromKey, toKey).values().iterator())
                                .collect(Collectors.toList()))
                )
        );
    }

    public int tablesSize() {
        return tables.size();
    }

    public int getCurrentSize() {
        return currentStorageMemoryUsage.get();
    }

    public int getFullMemoryUsage() {
        int memoryUsage = 0;
        for (MemoryWithSize mem : flushingStorages.values()) {
            memoryUsage += mem.getMemorySize();
        }
        return memoryUsage + getCurrentSize();
    }

    public void put(Record record) {
        currentStorageMemoryUsage.addAndGet(LsmDAO.sizeOf(record));
        currentStorage.put(record.getKey(), record);
    }
}
