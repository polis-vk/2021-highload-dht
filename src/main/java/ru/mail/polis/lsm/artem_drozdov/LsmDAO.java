package ru.mail.polis.lsm.artem_drozdov;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class LsmDAO implements DAO {

    private static final Logger LOG = LoggerFactory.getLogger(LsmDAO.class);
    private final ExecutorService executorFlush = Executors.newSingleThreadScheduledExecutor();

    private MemTable memTable;
    private final ConcurrentLinkedDeque<SSTable> tables = new ConcurrentLinkedDeque<>();
    public final ConcurrentLinkedQueue<MemTable> flushedMemTables = new ConcurrentLinkedQueue<>();

    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final DAOConfig config;

    private final AtomicInteger memoryConsumption = new AtomicInteger();
    private final AtomicInteger memTableWriters = new AtomicInteger();

    private final Object writeLock = new Object();
    private final AtomicInteger readersCount = new AtomicInteger();
    private final AtomicBoolean writerIsActive = new AtomicBoolean();

    /**
     * Create LsmDAO from config.
     *
     * @param config - LsmDAO config
     * @throws IOException - in case of io exception
     */
    public LsmDAO(DAOConfig config) throws IOException {
        this.config = config;
        List<SSTable> ssTables = SSTable.loadFromDir(config.dir);
        tables.addAll(ssTables);
        memTable = MemTable.newStorage(tables.size());
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        readersCount.incrementAndGet();
        if (writerIsActive.get()) {
            readersCount.decrementAndGet();
            synchronized (writeLock) {
                readersCount.incrementAndGet();
            }
        }

        try {
            return rangeImpl(fromKey, toKey);
        } finally {
            readersCount.decrementAndGet();
        }
    }

    @Override
    public void upsert(@Nonnull Record record) {
        upsertImpl(record);
    }

    private Iterator<Record> rangeImpl(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        Iterator<Record> sstableRanges = sstableRanges(tables, fromKey, toKey);
        Iterator<Record> memoryRange = map(memTable, fromKey, toKey).values().iterator();
        Iterator<Record> iterator = mergeTwo(new PeekingIterator(sstableRanges), new PeekingIterator(memoryRange));

        List<Iterator<Record>> flushedTables = new ArrayList<>(flushedMemTables.size());
        for (MemTable flushedMemTable : flushedMemTables) {
            Iterator<Record> flushMemoryRange = map(flushedMemTable, fromKey, toKey).values().iterator();
            flushedTables.add(flushMemoryRange);
        }

        iterator = mergeTwo(new PeekingIterator(iterator), new PeekingIterator(merge(flushedTables)));
        return filterTombstones(iterator);
    }

    public void upsertImpl(Record record) {
        memTableWriters.incrementAndGet();
        while (memoryConsumption.addAndGet(sizeOf(record)) > config.memoryLimit) {
            memTableWriters.decrementAndGet();

            synchronized (this) {
                // Если в данный блок попали друг за другом два и более потока, значит каждый из них
                // увеличил общий счетчик ранее, а первый пришедший сбросил его.
                // Для остальных потоков нужно снова его увеличить. С этим поможет while.
                if (memoryConsumption.get() > config.memoryLimit) {
                    // Применяем активное ожидание, чтобы избавиться от
                    // медленного и вредного потока, который никак не может
                    // записать своё значение в memTable после всех проверок.
                    waitMemTableWriters();
                    scheduleFlush(memTable);

                    memoryConsumption.getAndSet(sizeOf(record));
                    memTableWriters.incrementAndGet();
                    break;
                }
                memTableWriters.incrementAndGet();
            }
        }

        memTable.put(record.getKey(), record);
        memTableWriters.decrementAndGet();
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
            memTable = MemTable.newStorage(tables.size());
        }
    }

    /**
     * Активно и, что очень важно, недолго ожидаем медленные потоки,
     * которые ещё не завершили запись в memTable.
     * Например, применяется при выгрузке memTable в SSTable.
     */
    @GuardedBy("this")
    private void waitMemTableWriters() {
        while (memTableWriters.get() > 0) {
            Thread.onSpinWait();
        }
    }

    private int sizeOf(Record record) {
        return SSTable.sizeOf(record);
    }

    @Override
    public void close() {
        final Future<?> future;
        synchronized (this) {
            waitMemTableWriters();
            future = scheduleFlush(memTable);
        }

        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("flush error in close(): {}", e.getMessage(), e);
        }

        executorFlush.shutdown();
    }

    @GuardedBy("this")
    private Future<?> scheduleFlush(MemTable memTable) {
        flushedMemTables.add(memTable);
        Future<?> future = executorFlush.submit(() -> {
            synchronized (writeLock) {
                writerIsActive.set(true);
                while (readersCount.get() != 0) {
                    Thread.onSpinWait();
                }
                synchronized (this) {
                    makeFlush();
                }
                writerIsActive.set(false);
            }
        });
        this.memTable = MemTable.newStorage(memTable.getId() + 1);
        return future;
    }

    @GuardedBy("this")
    private void makeFlush() {
        MemTable flushMemTable = flushedMemTables.poll();
        if (flushMemTable == null) {
            return;
        }

        try {
            Path dir = config.dir;
            Path file = dir.resolve(SSTable.SSTABLE_FILE_PREFIX + flushMemTable.getId());

            SSTable ssTable = SSTable.write(flushMemTable.values().iterator(), file);
            tables.add(ssTable);
        } catch (IOException e) {
            flushedMemTables.add(flushMemTable);
            LOG.error("flush error in FLUSH_EXECUTOR: {}", e.getMessage(), e);
        }
    }

    private Iterator<Record> sstableRanges(
            Deque<SSTable> tables, @Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey
    ) {
        List<Iterator<Record>> iterators = new ArrayList<>(tables.size());
        for (SSTable ssTable : tables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return merge(iterators);
    }

    private SortedMap<ByteBuffer, Record> map(
            MemTable memTable, @Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey
    ) {
        if (fromKey == null && toKey == null) {
            return memTable;
        }
        if (fromKey == null) {
            return memTable.headMap(toKey);
        }
        if (toKey == null) {
            return memTable.tailMap(fromKey);
        }
        return memTable.subMap(fromKey, toKey);
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
        return new TombstonesFilterIterator(iterator);
    }
}
