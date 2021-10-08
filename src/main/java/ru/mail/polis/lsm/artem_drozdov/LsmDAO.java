package ru.mail.polis.lsm.artem_drozdov;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.anastasia_tushkanova.FlushService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LsmDAO implements DAO {
    private final ConcurrentLinkedDeque<SSTable> tables = new ConcurrentLinkedDeque<>();
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final DAOConfig config;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final FlushService flushService;

    private int memoryConsumption;
    private NavigableMap<ByteBuffer, Record> memoryStorage = newStorage();

    public LsmDAO(DAOConfig config) throws IOException {
        this.config = config;
        List<SSTable> ssTables = SSTable.loadFromDir(config.dir);
        tables.addAll(ssTables);
        this.flushService = new FlushService(config, tables.size(), newTable -> {
            lock.writeLock().lock();
            tables.add(newTable);
            lock.writeLock().unlock();
        });
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        lock.readLock().lock();
        try {
            Iterator<Record> sstableRanges = sstableRanges(fromKey, toKey);
            Iterator<Record> memoryRange = map(fromKey, toKey, memoryStorage).values().iterator();
            Iterator<Record> waitingStoragesRange = getWaitingStoragesRange(fromKey, toKey);
            Iterator<Record> iterator = merge(List.of(sstableRanges, waitingStoragesRange, memoryRange));
            return filterTombstones(iterator);
        } finally {
            lock.readLock().unlock();
        }
    }

    private Iterator<Record> getWaitingStoragesRange(@Nullable ByteBuffer fromKey,
                                                     @Nullable ByteBuffer toKey) {
        List<NavigableMap<ByteBuffer, Record>> waitingToFlushStorages = flushService.getWaitingToFlushStorages();
        List<Iterator<Record>> iterators = new ArrayList<>(waitingToFlushStorages.size());
        for (NavigableMap<ByteBuffer, Record> currentMemoryStorage : waitingToFlushStorages) {
            iterators.add(map(fromKey, toKey, currentMemoryStorage).values().iterator());
        }
        return merge(iterators);
    }

    @Override
    public void upsert(Record record) {
        lock.readLock().lock();
        try {
            memoryConsumption += sizeOf(record);
            if (memoryConsumption > config.memoryLimit) {
                flushService.submit(memoryStorage);
                memoryStorage = new ConcurrentSkipListMap<>();
                memoryConsumption = sizeOf(record);
            }
            memoryStorage.put(record.getKey(), record);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public synchronized void closeAndCompact() {
        flushService.resetTablesCount();
        SSTable table;
        try {
            table = SSTable.compact(config.dir, range(null, null));
        } catch (IOException e) {
            throw new UncheckedIOException("Can't compact", e);
        }
        tables.clear();
        tables.add(table);
        memoryStorage = newStorage();
    }

    private NavigableMap<ByteBuffer, Record> newStorage() {
        return new ConcurrentSkipListMap<>();
    }

    private int sizeOf(Record record) {
        return SSTable.sizeOf(record);
    }

    @Override
    public synchronized void close() throws IOException {
        flushService.submit(memoryStorage);
        flushService.close();
    }

    private Iterator<Record> sstableRanges(@Nullable ByteBuffer fromKey,
                                           @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(tables.size());
        for (SSTable ssTable : tables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return merge(iterators);
    }

    private SortedMap<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey,
                                              @Nullable ByteBuffer toKey,
                                              @Nonnull NavigableMap<ByteBuffer, Record> memoryStorage) {
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

    private static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        if (iterators.isEmpty()) {
            return Collections.emptyIterator();
        }
        if (iterators.size() == 1) {
            return iterators.get(0);
        }
        if (iterators.size() == 2) {
            return mergeTwo(new PeekingIterator(iterators.get(0)), new PeekingIterator(iterators.get(1)));
        }
        Iterator<Record> left = merge(iterators.subList(0, iterators.size() / 2));
        Iterator<Record> right = merge(iterators.subList(iterators.size() / 2, iterators.size()));
        return mergeTwo(new PeekingIterator(left), new PeekingIterator(right));
    }

    private static Iterator<Record> mergeTwo(PeekingIterator left, PeekingIterator right) {
        return new MergeTwoIterator(left, right);
    }

    private static Iterator<Record> filterTombstones(Iterator<Record> iterator) {
        PeekingIterator delegate = new PeekingIterator(iterator);
        return new TombstoneFilterIterator(delegate);
    }
}
