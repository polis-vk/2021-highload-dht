package ru.mail.polis.lsm.eldar_tim;

import one.nio.async.CompletedFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.eldar_tim.components.LimitedMemTable;
import ru.mail.polis.lsm.eldar_tim.components.MemTable;
import ru.mail.polis.lsm.eldar_tim.components.ReadonlyMemTable;
import ru.mail.polis.lsm.eldar_tim.components.SSTable;
import ru.mail.polis.lsm.eldar_tim.components.Storage;
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
import static ru.mail.polis.lsm.eldar_tim.Utils.map;
import static ru.mail.polis.lsm.eldar_tim.Utils.mergeTwo;
import static ru.mail.polis.lsm.eldar_tim.Utils.sstableRanges;
import static ru.mail.polis.lsm.eldar_tim.components.SSTable.sizeOf;

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

    private Iterator<Record> rangeImpl(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        Iterator<Record> sstableRanges = sstableRanges(storage.sstables, fromKey, toKey);

        Iterator<Record> flushingMemTableIterator = map(storage.memTableToFlush, fromKey, toKey).values().iterator();
        Iterator<Record> memTableIterator = map(storage.memTable, fromKey, toKey).values().iterator();
        Iterator<Record> memoryRanges = mergeTwo(flushingMemTableIterator, memTableIterator);

        Iterator<Record> iterator = mergeTwo(sstableRanges, memoryRanges);
        return new TombstonesFilterIterator(iterator);
    }

    @Override
    public void upsert(@Nonnull Record record) {
//      FIXME:
//        if (serverIsDown) {
//            throw new ServerNotActiveExc();
//        }

        int recordSize = sizeOf(record);
        while (true) {
            LimitedMemTable limitedMemTable = this.storage.memTable;

            if (limitedMemTable.reserveSize(recordSize)) {
                limitedMemTable.put(record, recordSize);
                break;
            } else if (limitedMemTable.requestFlush()) {
                flush();
            }
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
            memTable.set(ReadonlyMemTable.newStorage(tables.size()));
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

    private synchronized void flush() {
        waitForFlushingComplete();

        Storage flushing = storage;
        storage = flushing.stateReadyToFlush();

        flushingFuture = executorFlush.submit(() -> {
            SSTable flushResult = flushImpl(storage.memTableToFlush);
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

    @GuardedBy("this")
    private void waitForFlushingComplete() {
        try {
            flushingFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("flush future wait error: {}", e.getMessage(), e);
        }
    }

    private SSTable flushImpl(MemTable memTable) {
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
