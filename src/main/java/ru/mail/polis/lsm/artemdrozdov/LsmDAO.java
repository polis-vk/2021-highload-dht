package ru.mail.polis.lsm.artemdrozdov;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class LsmDAO implements DAO {

    private NavigableMap<ByteBuffer, Record> memoryStorage = newStorage();
    private final ConcurrentLinkedDeque<SSTable> tables = new ConcurrentLinkedDeque<>();

    private final DAOConfig config;

    private final AtomicLong memoryConsumption;
    private volatile AtomicInteger semaphoreAvailablePermits;
    private volatile AtomicBoolean wantToClose;
    private volatile AtomicLong tableSize;
    private volatile Semaphore semaphore;

    /**
     *  Create LsmDAO from config.
     *
     * @param config - LamDAo config
     * @throws IOException - in case of io exception
     */
    public LsmDAO(DAOConfig config, final int semaphorePermit) throws IOException {
        this.memoryConsumption = new AtomicLong();
        this.wantToClose = new AtomicBoolean(false);
        this.semaphoreAvailablePermits = new AtomicInteger(semaphorePermit);
        this.semaphore = new Semaphore(semaphorePermit);
        this.config = config;
        List<SSTable> ssTables = SSTable.loadFromDir(config.dir);
        tables.addAll(ssTables);
        tableSize = new AtomicLong(tables.size());
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        Iterator<Record> sstableRanges = sstableRanges(fromKey, toKey);
        Iterator<Record> memoryRange = map(fromKey, toKey).values().iterator();
        Iterator<Record> iterator = mergeTwo(new PeekingIterator(sstableRanges), new PeekingIterator(memoryRange));
        return filterTombstones(iterator);
    }

    public boolean greaterThanCAS(final int maxValue, final int size) {
        return (memoryConsumption.getAndUpdate(x -> (x + size) > maxValue ? size : x) + size) > maxValue;
    }

    @Override
    public void upsert(Record record) {
        if (greaterThanCAS(config.memoryLimit, sizeOf(record)) && !this.wantToClose.get()) {

            try {
                this.semaphore.acquire();
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }

            NavigableMap<ByteBuffer, Record> flushStorage = newStorage();
            flushStorage.putAll(memoryStorage);

            CompletableFuture.runAsync(() -> {
                final int rollbackSize = sizeOf(record);
                try {
                    this.flush(flushStorage);
                    flushStorage.keySet().retainAll(memoryStorage.keySet());
                    memoryStorage.values().removeAll(flushStorage.values());
                } catch (IOException e) {
                    memoryConsumption.addAndGet(-rollbackSize);
                    memoryStorage.putAll(flushStorage); // restore data + new data
                } finally {
                    this.semaphore.release();
                    if (this.wantToClose.get()
                            && this.semaphoreAvailablePermits.compareAndSet(this.semaphore.availablePermits(), 0)) {
                        synchronized (this.semaphore) {
                            this.semaphore.notifyAll();
                        }
                    }
                }
            });
        } else {
            memoryConsumption.addAndGet(sizeOf(record));
        }


        memoryStorage.put(record.getKey(), record);
    }

    @Override
    public void closeAndCompact() {
        synchronized (this) {
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
    }

    private NavigableMap<ByteBuffer, Record> newStorage() {
        return new ConcurrentSkipListMap<>();
    }

    private int sizeOf(Record record) {
        return SSTable.sizeOf(record);
    }

    @Override
    public void close() throws IOException {
        this.wantToClose.set(true);
        if (this.semaphoreAvailablePermits.get() != this.semaphore.availablePermits()) {
            synchronized (this.semaphore) {
                try {
                    // это цикл попросил sonar-java
                    while (this.semaphoreAvailablePermits.get() != 0) {
                        this.semaphore.wait();
                    }
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
        }
        flush(memoryStorage);
    }

    private void flush(NavigableMap<ByteBuffer, Record> flushStorage) throws IOException {
        Path dir = config.dir;
        Path file = dir.resolve(SSTable.SSTABLE_FILE_PREFIX + tableSize.getAndAdd(1));
        SSTable ssTable = SSTable.write(flushStorage.values().iterator(), file);
        tables.add(ssTable);
    }

    private Iterator<Record> sstableRanges(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(tables.size());
        for (SSTable ssTable : tables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return merge(iterators);
    }

    private SortedMap<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
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
        return new MergeIterator(left, right);
    }

    private static Iterator<Record> filterTombstones(Iterator<Record> iterator) {
        PeekingIterator delegate = new PeekingIterator(iterator);
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                for (;;) {
                    Record peek = delegate.peek();
                    if (peek == null) {
                        return false;
                    }
                    if (!peek.isTombstone()) {
                        return true;
                    }

                    delegate.next();
                }
            }

            @Override
            public Record next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("No elements");
                }
                return delegate.next();
            }
        };
    }

}
