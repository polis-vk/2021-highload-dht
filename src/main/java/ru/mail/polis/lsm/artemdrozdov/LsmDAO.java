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
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class LsmDAO implements DAO {

    private final NavigableMap<ByteBuffer, Record> memoryStorage = newStorage();
    private final Semaphore semaphore;
    private TableStorage tableStorage;

    private final ExecutorService flushExecutor = Executors.newSingleThreadExecutor();
    private final ExecutorService compactExecutor = Executors.newSingleThreadExecutor();

    private final DAOConfig config;

    private final AtomicLong memoryConsumption;
    // общее количество таблиц
    private final AtomicLong tableCounter;
    // таблицы которые успели зафлашиться, перед компактом
    private final AtomicInteger sizeBeforeCompact;

    /**
     *  Create LsmDAO from config.
     *
     * @param config - LamDAo config
     * @param permits - каунтер для семафора; слишком большое значение ведет к OutOfMemory Exception
     * @throws IOException - in case of io exception
     */
    public LsmDAO(DAOConfig config, final int permits) throws IOException {
        this.memoryConsumption = new AtomicLong();
        this.semaphore = new Semaphore(permits);
        this.config = config;
        List<SSTable> ssTables = SSTable.loadFromDir(config.dir);
        this.tableStorage = new TableStorage(ssTables);
        this.tableCounter = new AtomicLong(this.tableStorage.tables.size());
        this.sizeBeforeCompact = new AtomicInteger(0);
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        Iterator<Record> sstableRanges = sstableRanges(fromKey, toKey);
        Iterator<Record> memoryRange = map(fromKey, toKey).values().iterator();
        Iterator<Record> iterator = mergeTwo(sstableRanges, memoryRange);
        return filterTombstones(iterator);
    }

    public boolean greaterThanCAS(final int maxSize, final int newSize) {
        return (memoryConsumption.getAndUpdate(val -> (val + newSize) > maxSize ? newSize : val) + newSize) > maxSize;
    }

    @Override
    public void upsert(Record record) {

        if (greaterThanCAS(config.memoryLimit, sizeOf(record))) {

            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                putRecord(record);
                Thread.currentThread().interrupt();
                return;
            }

            flushExecutor.execute(() -> {
                final int rollbackSize = sizeOf(record);
                NavigableMap<ByteBuffer, Record> flushStorage = newStorage();
                try {
                        flushStorage.putAll(memoryStorage);
                        SSTable flushTable = flush(flushStorage);
                        this.tableStorage = tableStorage.afterFlush(flushTable);
                        // Удаление записи у одинаковых ключей (и ключ не был изменен во время флаша)
                        flushStorage.forEach((key, value) -> {
                            Record tmp = memoryStorage.get(key);
                            if (tmp != null) {
                                if (tmp.getValue().equals(value.getValue())) {
                                    memoryStorage.remove(key);
                                }
                            }
                        });
                } catch (IOException e) {
                    memoryConsumption.addAndGet(-rollbackSize);
                    memoryStorage.putAll(flushStorage); // restore data + new data
                    Thread.currentThread().interrupt();
                } finally {
                    semaphore.release();
                }
            });

            compactExecutor.execute(() -> {
                synchronized (this) {
                    if (tableStorage.isCompact(config.tableLimit)) {
                        compact();
                    }
                }
            });

        } else {
            memoryConsumption.addAndGet(sizeOf(record));
        }

        putRecord(record);
    }

    @Override
    public void compact() {
        try {
            sizeBeforeCompact.set(tableStorage.tables.size());
            SSTable table = perfomCompact();
            this.tableStorage = tableStorage.afterCompact(table, sizeBeforeCompact.get());
        } catch (IOException e) {
            throw new UncheckedIOException("Can't compact", e);
        }
    }

    private SSTable perfomCompact() throws IOException {
        return SSTable.compact(config.dir, range(null, null));
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
        flushExecutor.shutdown();
        compactExecutor.shutdown();
        try {
            if (!flushExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("Error! FlushExecutor Await termination in close...");
            }
            if (!compactExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("Error! CompactExecutor Await termination in close...");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }

        synchronized (this) {
            flush(memoryStorage);
            if (tableStorage.isCompact(config.tableLimit)) {
                compact();
            }
        }
    }

    private SSTable flush(NavigableMap<ByteBuffer, Record> flushStorage) throws IOException {
        Path dir = config.dir;
        Path file = dir.resolve(SSTable.SSTABLE_FILE_PREFIX + tableCounter.getAndAdd(1));
        return SSTable.write(flushStorage.values().iterator(), file);
    }

    private Iterator<Record> sstableRanges(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(tableStorage.tables.size());
        for (SSTable ssTable : tableStorage.tables) {
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

    /**
     * some doc.
     */
    public static Iterator<Record> merge(List<Iterator<Record>> iterators) {
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

    public static Iterator<Record> mergeTwo(Iterator<Record> left, Iterator<Record> right) {
        return new MergeIterator(new PeekingIterator(left), new PeekingIterator(right));
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
