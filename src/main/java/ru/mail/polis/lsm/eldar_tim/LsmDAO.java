package ru.mail.polis.lsm.eldar_tim;

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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

import static ru.mail.polis.lsm.eldar_tim.components.Utils.map;
import static ru.mail.polis.lsm.eldar_tim.components.Utils.mergeTwo;
import static ru.mail.polis.lsm.eldar_tim.components.Utils.sstableRanges;
import static ru.mail.polis.lsm.eldar_tim.components.SSTable.sizeOf;

//@SuppressWarnings({"PMD", "JdkObsolete"}) FIXME
@SuppressWarnings("NonAtomicOperationOnVolatileField")
public class LsmDAO implements DAO {

    private static final Logger LOG = LoggerFactory.getLogger(LsmDAO.class);

    private final AwaitableExecutor executorFlush = new AwaitableExecutor();

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
        Storage storage = this.storage;
        isActiveOrThrow();

        Iterator<Record> sstableRanges = sstableRanges(storage.sstables, fromKey, toKey);

        Iterator<Record> flushingMemTableIterator = map(storage.memTableToFlush, fromKey, toKey).values().iterator();
        Iterator<Record> memTableIterator = map(storage.memTable, fromKey, toKey).values().iterator();
        Iterator<Record> memoryRanges = mergeTwo(flushingMemTableIterator, memTableIterator);

        Iterator<Record> iterator = mergeTwo(sstableRanges, memoryRanges);
        return new TombstonesFilterIterator(iterator);
    }

    @Override
    public void upsert(@Nonnull Record record) {
        int recordSize = sizeOf(record);
        while (isActiveOrThrow()) {
            LimitedMemTable limitedMemTable = this.storage.memTable;

            if (limitedMemTable.reserveSize(recordSize)) {
                limitedMemTable.put(record, recordSize);
                break;
            } else if (limitedMemTable.requestFlush()) {
                scheduleFlush();
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
    public void close() throws IOException {
        LOG.info("{} is closing...", getClass().getName());

        serverIsDown = true;

        executorFlush.await();
        executorFlush.shutdown();

        storage = storage.beforeFlush();
        flush(storage.memTableToFlush);
        storage = null;

        LOG.info("{} closed", getClass().getName());
    }

    private synchronized void scheduleFlush() {
        LOG.debug("Preparing to flush...");
        executorFlush.await();

        Storage flushing = storage;
        storage = flushing.beforeFlush();

        executorFlush.execute(() -> {
            try {
                LOG.debug("Flushing...");
                SSTable flushedTable = flush(flushing.memTableToFlush);
                storage = storage.afterFlush(flushedTable);
                LOG.debug("Flush completed");
            } catch (IOException e) {
                LOG.error("Flush error: {}", e.getMessage(), e);
            }
        });
    }

    private SSTable flush(MemTable memTable) throws IOException {
        Path dir = config.dir;
        Path file = dir.resolve(SSTable.SSTABLE_FILE_PREFIX + memTable.getId());
        return SSTable.write(memTable.raw().values().iterator(), file);
    }

    private boolean isActiveOrThrow() {
        if (serverIsDown) {
            throw new ServerNotActiveExc();
        }
        return true;
    }
}
