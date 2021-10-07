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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class LsmDAO implements DAO {

    private NavigableMap<ByteBuffer, Record> memoryStorage = newStorage();
    private final List<NavigableMap<ByteBuffer, Record>> circularBuffer;
    private final ConcurrentLinkedDeque<SSTable> tables = new ConcurrentLinkedDeque<>();

    private final DAOConfig config;

    private final AtomicLong memoryConsumption;
    private final AtomicInteger semaphoreAvailablePermits;
    private final AtomicInteger idCircularBuffer;
    private final AtomicBoolean wantToClose;
    private final AtomicLong tableSize;
    private final Semaphore semaphore;

    /**
     *  Create LsmDAO from config.
     *
     * @param config - LamDAo config
     * @param semaphorePermit - каунтер для семафора; слишком большое значение ведет к OutOfMemory Exception
     * @throws IOException - in case of io exception
     */
    public LsmDAO(DAOConfig config, final int semaphorePermit) throws IOException {
        System.out.println(semaphorePermit);
        this.memoryConsumption = new AtomicLong();
        this.idCircularBuffer = new AtomicInteger(0);
        this.wantToClose = new AtomicBoolean(false);
        this.semaphoreAvailablePermits = new AtomicInteger(semaphorePermit);
        this.semaphore = new Semaphore(semaphorePermit);
        this.circularBuffer = new CopyOnWriteArrayList<>();
        for (int i = 0; i < semaphorePermit; ++i) {
            this.circularBuffer.add(newStorage());
        }
        this.config = config;
        List<SSTable> ssTables = SSTable.loadFromDir(config.dir);
        tables.addAll(ssTables);
        tableSize = new AtomicLong(tables.size());
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        Iterator<Record> sstableRanges = sstableRanges(fromKey, toKey);
        Iterator<Record> memoryRange = map(fromKey, toKey, memoryStorage).values().iterator();

        Iterator<NavigableMap<ByteBuffer, Record>> curcilarIterator = this.circularBuffer.iterator();

        if (!this.circularBuffer.isEmpty()) {
            while (curcilarIterator.hasNext()) {
                Iterator<Record> unionRange = map(fromKey, toKey, curcilarIterator.next()).values().iterator();
                memoryRange = mergeTwo(new PeekingIterator(memoryRange), new PeekingIterator(unionRange));
            }
        }

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
                // здесь можно попробовать переделать на tryAcquire
                // отпускать поток, записав данные в memStore
                this.semaphore.acquire();
            } catch (InterruptedException e) {
                putRecord(record);
                Thread.currentThread().interrupt();
            }

            if (this.wantToClose.get()) {
                putRecord(record);
                this.semaphore.release();
                return;
            }

            final int idx = idCircularBuffer.getAndUpdate(i -> (i + 1) % this.semaphoreAvailablePermits.get());
            circularBuffer.set(idx, newStorage());
            circularBuffer.get(idx).putAll(memoryStorage);
            memoryStorage = newStorage();

            CompletableFuture.runAsync(() -> {
                final int localIdx = idx;
                final int rollbackSize = sizeOf(record);
                try {
                    this.flush(circularBuffer.get(localIdx));
                } catch (IOException e) {
                    memoryConsumption.addAndGet(-rollbackSize);
                    memoryStorage.putAll(circularBuffer.get(localIdx)); // restore data + new data
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

        putRecord(record);
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

    private void putRecord(Record record) {
        memoryStorage.put(record.getKey(), record);
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
                    // sonar-java
                    while (this.semaphoreAvailablePermits.get() != 0) {
                        this.semaphore.wait(250);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        this.circularBuffer.clear();
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

    private SortedMap<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey,
                                              final NavigableMap<ByteBuffer, Record> storage) {
        if (fromKey == null && toKey == null) {
            return storage;
        }
        if (fromKey == null) {
            return storage.headMap(toKey);
        }
        if (toKey == null) {
            return storage.tailMap(fromKey);
        }
        return storage.subMap(fromKey, toKey);
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
