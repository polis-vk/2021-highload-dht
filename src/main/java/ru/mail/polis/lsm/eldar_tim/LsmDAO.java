package ru.mail.polis.lsm.eldar_tim;

import one.nio.async.CompletedFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.eldar_tim.iterators.TombstonesFilterIterator;
import ru.mail.polis.service.exceptions.ServerNotActiveExc;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static ru.mail.polis.ServiceUtils.shutdownAndAwaitExecutor;
import static ru.mail.polis.lsm.eldar_tim.SSTable.sizeOf;
import static ru.mail.polis.lsm.eldar_tim.Utils.map;
import static ru.mail.polis.lsm.eldar_tim.Utils.mergeTwo;
import static ru.mail.polis.lsm.eldar_tim.Utils.sstableRanges;

@SuppressWarnings({"PMD", "JdkObsolete"})
public class LsmDAO implements DAO {

    private static final Logger LOG = LoggerFactory.getLogger(LsmDAO.class);

    private final ExecutorService executorFlush = Executors.newSingleThreadScheduledExecutor();
    private Future<?> flushingFuture = new CompletedFuture<>(null);

    private final DAOConfig config;

    private volatile Storage storage;
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
        storage = new Storage(ssTables, config.memoryLimit);
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        rangeRWLock.readLock().lock();
        try {
            if (serverIsDown) {
                throw new ServerNotActiveExc();
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
        Iterator<Record> sstableRanges = sstableRanges(storage.sstables, fromKey, toKey);

        Iterator<Record> flushingMemTableIterator = map(storage.memTableToFlush, fromKey, toKey).values().iterator();
        Iterator<Record> memTableIterator = map(storage.memTable, fromKey, toKey).values().iterator();
        Iterator<Record> memoryRanges = mergeTwo(flushingMemTableIterator, memTableIterator);

        Iterator<Record> iterator = mergeTwo(sstableRanges, memoryRanges);
        return new TombstonesFilterIterator(iterator);
    }

    public void upsertImpl(Record record) {
        var storage = this.storage;
        var memoryConsumption = storage.memoryConsumption;
        var recordSize = sizeOf(record);

        while (memoryConsumption.addAndGet(recordSize) > config.memoryLimit) {
            synchronized (this) {
                if (memoryConsumption.get() > config.memoryLimit) {
                    if (serverIsDown) {
                        throw new ServerNotActiveExc();
                    }

                    scheduleFlush();
                    memoryConsumption.getAndSet(recordSize);
                    break;
                } else {
                    memoryConsumption.addAndGet(-recordSize);
                }
            }
        }

        if (serverIsDown) {
            throw new ServerNotActiveExc();
        }

        while (true) {
            var limitedMemTable = this.storage.memTable;

            if (limitedMemTable.reserveSize(recordSize)) {
                limitedMemTable.put(record.getKey(), record);
                break;
            } else if (limitedMemTable.requestFlush()) {
                // FIXME: do flush
            }
        }

        storage.memTable.put(record.getKey(), record);
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

        upsertRWLock.writeLock().lock();
        try {
            serverIsDown = true;
            scheduleFlush();
            waitForFlushingComplete();
        } finally {
            upsertRWLock.writeLock().unlock();
            shutdownAndAwaitExecutor(executorFlush, LOG);
        }

        LOG.info("{} closed", getClass().getName());
    }

    @GuardedBy("upsertRWLock")
    private void scheduleFlush() {
        waitForFlushingComplete();

        MemTable flushingTable = memTable.get();
        flushingMemTable.set(flushingTable);
        memTable.set(MemTable.newStorage(flushingTable.getId() + 1));

        assert !rangeRWLock.isWriteLockedByCurrentThread();

        flushingFuture = executorFlush.submit(() -> {
            SSTable flushResult = flush(flushingTable);
            if (flushResult == null) {
                // Restoring not flushed data.
                flushingTable.putAll(memTable.get());
                memTable.set(flushingTable);
                return;
            }
            rangeRWLock.writeLock().lock();
            try {
                tables.add(flushResult);
            } finally {
                rangeRWLock.writeLock().unlock();
            }
        });
    }

    @GuardedBy("upsertRWLock")
    private void waitForFlushingComplete() {
        // Protects flushingFuture variable.
        assert upsertRWLock.isWriteLockedByCurrentThread();

        try {
            flushingFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("flush future wait error: {}", e.getMessage(), e);
        }
    }

    private SSTable flush(MemTable memTable) {
        try {
            LOG.debug("Flushing...");

            Path dir = config.dir;
            Path file = dir.resolve(SSTable.SSTABLE_FILE_PREFIX + memTable.getId());

            return SSTable.write(memTable.values().iterator(), file);
        } catch (IOException e) {
            LOG.error("flush error: {}", e.getMessage(), e);
            return null;
        } finally {
            LOG.debug("Flushing completed");
        }
    }
}
