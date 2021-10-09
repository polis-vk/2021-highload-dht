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
import ru.mail.polis.service.exceptions.ServiceNotActiveException;

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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static ru.mail.polis.ServiceUtils.shutdownAndAwaitExecutor;

public class LsmDAO implements DAO {

    private static final Logger LOG = LoggerFactory.getLogger(LsmDAO.class);

    private final DAOConfig config;

    private final ExecutorService executorFlush = Executors.newSingleThreadScheduledExecutor();
    private Future<?> flushingFuture = new CompletedFuture<>(null);

    private final AtomicReference<MemTable> memTable = new AtomicReference<>();
    private final AtomicReference<MemTable> flushingMemTable = new AtomicReference<>();
    private final ConcurrentLinkedDeque<SSTable> tables = new ConcurrentLinkedDeque<>();

    private final AtomicInteger memoryConsumption = new AtomicInteger();

    private final ReentrantReadWriteLock rangeRWLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock upsertRWLock = new ReentrantReadWriteLock();

    @SuppressWarnings("PMD.AvoidUsingVolatile")
    private volatile boolean serverIsDown;

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
        flushingMemTable.set(MemTable.newStorage(tables.size() + 1));
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        rangeRWLock.readLock().lock();
        try {
            if (serverIsDown) {
                throw new ServiceNotActiveException();
            }

            return rangeImpl(fromKey, toKey);
        } finally {
            rangeRWLock.readLock().unlock();
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
        upsertRWLock.readLock().lock();
        while (memoryConsumption.addAndGet(sizeOf(record)) > config.memoryLimit) {
            upsertRWLock.readLock().unlock();
            synchronized (this) {
                if (memoryConsumption.get() > config.memoryLimit) {
                    upsertRWLock.writeLock().lock();
                    try {
                        if (serverIsDown) {
                            throw new ServiceNotActiveException();
                        }

                        scheduleFlush();
                        memoryConsumption.getAndSet(sizeOf(record));
                        break;
                    } finally {
                        upsertRWLock.readLock().lock();
                        upsertRWLock.writeLock().unlock();
                    }
                } else {
                    upsertRWLock.readLock().lock();
                }
            }
        }

        try {
            if (serverIsDown) {
                throw new ServiceNotActiveException();
            }

            memTable.get().put(record.getKey(), record);
        } finally {
            upsertRWLock.readLock().unlock();
        }
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

    @Override
    public void close() {
        LOG.info("{} is closing...", getClass().getName());

        rangeRWLock.writeLock().lock();
        upsertRWLock.writeLock().lock();
        try {
            serverIsDown = true;
            scheduleFlush();
        } finally {
            upsertRWLock.writeLock().unlock();
            rangeRWLock.writeLock().unlock();
        }

        waitForFlushingComplete();
        shutdownAndAwaitExecutor(executorFlush, LOG);

        LOG.info("{} closed", getClass().getName());
    }

    @GuardedBy("upsertRWLock")
    private void scheduleFlush() {
        assert upsertRWLock.isWriteLockedByCurrentThread();

        waitForFlushingComplete();

        MemTable flushingTable = memTable.get();
        flushingMemTable.set(flushingTable);
        memTable.set(MemTable.newStorage(flushingTable.getId() + 1));

        flushingFuture = executorFlush.submit(() -> {
            rangeRWLock.writeLock().lock();
            try {
                flush(flushingTable);
            } finally {
                rangeRWLock.writeLock().unlock();
            }
        });
    }

    @GuardedBy("upsertRWLock")
    private void waitForFlushingComplete() {
        try {
            flushingFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("flush future wait error: {}", e.getMessage(), e);
        }
    }

    private void flush(MemTable memTable) {
        try {
            LOG.debug("Flushing...");

            Path dir = config.dir;
            Path file = dir.resolve(SSTable.SSTABLE_FILE_PREFIX + memTable.getId());

            SSTable ssTable = SSTable.write(memTable.values().iterator(), file);
            tables.add(ssTable);

            LOG.debug("Flushing completed");
        } catch (IOException e) {
            LOG.error("flush error: {}", e.getMessage(), e);
        }
    }

    private int sizeOf(Record record) {
        return SSTable.sizeOf(record);
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
