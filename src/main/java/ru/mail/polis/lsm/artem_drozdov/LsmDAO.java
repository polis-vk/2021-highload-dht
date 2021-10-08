package ru.mail.polis.lsm.artem_drozdov;

import one.nio.async.CompletedFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.artem_drozdov.iterators.MergeIterator;
import ru.mail.polis.lsm.artem_drozdov.iterators.PeekingIterator;
import ru.mail.polis.lsm.artem_drozdov.iterators.TombstonesFilterIterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LsmDAO implements DAO {

    private static final Logger LOG = LoggerFactory.getLogger(LsmDAO.class);

    private final AtomicReference<MemTable> memTable = new AtomicReference<>();
    private final AtomicReference<MemTable> flushingMemTable = new AtomicReference<>();
    private final ConcurrentLinkedDeque<SSTable> tables = new ConcurrentLinkedDeque<>();

    private Future<?> flushingFuture = new CompletedFuture<>(null);
    private final ExecutorService executorFlush = Executors.newSingleThreadScheduledExecutor();

    private final DAOConfig config;

    private final AtomicInteger memoryConsumption = new AtomicInteger();
    private final AtomicInteger memTableWriters = new AtomicInteger();

    private final ReadWriteLock dataRWLock = new ReentrantReadWriteLock();
    private final ReadWriteLock memTableRWLock = new ReentrantReadWriteLock();

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
        memTable.set(MemTable.newStorage(tables.size()));
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        dataRWLock.readLock().lock();
        try {
            return rangeImpl(fromKey, toKey);
        } finally {
            dataRWLock.readLock().unlock();
        }
    }

    @Override
    public void upsert(@Nonnull Record record) {
        upsertImpl(record);
    }

    private Iterator<Record> rangeImpl(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        Iterator<Record> sstableRanges = sstableRanges(tables, fromKey, toKey);

        Iterator<Record> flushingMemTableIterator = map(flushingMemTable.get(), fromKey, toKey).values().iterator();
        Iterator<Record> memTableIterator = map(memTable.get(), fromKey, toKey).values().iterator();
        Iterator<Record> memoryRanges = mergeTwo(flushingMemTableIterator, memTableIterator);

        Iterator<Record> iterator = mergeTwo(sstableRanges, memoryRanges);
        return filterTombstones(iterator);
    }

    public void upsertImpl(Record record) {
        memTableWriters.incrementAndGet();
        while (memoryConsumption.addAndGet(sizeOf(record)) > config.memoryLimit) {
            memTableWriters.decrementAndGet();

            // Если в данный блок попали друг за другом два и более потока, значит каждый из них
            // увеличил общий счетчик ранее, а первый зашедший в synchronized сбросил его.
            // Для остальных потоков нужно снова его увеличить. С этим поможет while.
            synchronized (this) {
                if (memoryConsumption.get() > config.memoryLimit) {
                    // Применяем активное ожидание, чтобы избавиться от
                    // медленного и вредного потока, который никак не может
                    // записать своё значение в memTable после всех проверок.
                    waitMemTableWriters();
                    scheduleFlush();

                    memoryConsumption.getAndSet(sizeOf(record));
                    memTableWriters.incrementAndGet();
                    break;
                }
                memTableWriters.incrementAndGet();
            }
        }

        memTable.get().put(record.getKey(), record);
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
            memTable.set(MemTable.newStorage(tables.size()));
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
        synchronized (this) {
            waitMemTableWriters();
            scheduleFlush();
        }

        executorFlush.shutdown();
    }

    @GuardedBy("this")
    private void scheduleFlush() {
        try {
            flushingFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("flush future wait error: {}", e.getMessage(), e);
        }

        assert flushingMemTable.get() == null;

        MemTable flushingTable = memTable.get();
        flushingMemTable.set(flushingTable);
        memTable.set(MemTable.newStorage(flushingTable.getId() + 1));

        flushingFuture = executorFlush.submit(() -> {
            dataRWLock.writeLock().lock();
            try {
                flush();
            } finally {
                dataRWLock.writeLock().unlock();
            }
        });
    }

    private void flush() {
        try {
            Path dir = config.dir;
            Path file = dir.resolve(SSTable.SSTABLE_FILE_PREFIX + flushingMemTable.get().getId());

            SSTable ssTable = SSTable.write(flushingMemTable.get().values().iterator(), file);
            tables.add(ssTable);

            flushingMemTable.set(null);
        } catch (IOException e) {
            LOG.error("flush error: {}", e.getMessage(), e);
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
            return mergeTwo(iterators.get(0), iterators.get(1));
        }
        Iterator<Record> left = merge(iterators.subList(0, iterators.size() / 2));
        Iterator<Record> right = merge(iterators.subList(iterators.size() / 2, iterators.size()));
        return mergeTwo(left, right);
    }

    private static Iterator<Record> mergeTwo(Iterator<Record> left, Iterator<Record> right) {
        return new MergeIterator(new PeekingIterator(left), new PeekingIterator(right));
    }

    private static Iterator<Record> filterTombstones(Iterator<Record> iterator) {
        return new TombstonesFilterIterator(iterator);
    }
}
