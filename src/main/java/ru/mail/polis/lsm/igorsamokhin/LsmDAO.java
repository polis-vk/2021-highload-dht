package ru.mail.polis.lsm.igorsamokhin;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("JdkObsolete")
public class LsmDAO implements DAO {
    @SuppressWarnings("PMD.AvoidUsingVolatile")//Storage is final class
    private volatile Storage storage;
    private final DAOConfig config;
    private final AtomicInteger memoryConsumption = new AtomicInteger(0);

    private final AtomicInteger fileN;

    private final ExecutorService executors = Executors.newSingleThreadExecutor();

    /**
     * Create DAO object.
     *
     * @param config - objects contains directory with data files
     */
    public LsmDAO(DAOConfig config) throws IOException {
        this.config = config;

        List<SSTable> ssTables = SSTable.loadFromDir(config.dir);
        fileN = new AtomicInteger(ssTables.size());
        storage = Storage.init(ssTables);
    }

    private String getNewFileName() {
        int size = fileN.getAndIncrement();
        return FileUtils.formatFileName(null, size, null);
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return this.storage.range(fromKey, toKey);
    }

    @Override
    public void upsert(Record record) {
        int size = record.size();
        if (memoryConsumption.addAndGet(size) > config.memoryLimit) {
            synchronized (this) {
                if (memoryConsumption.get() > config.memoryLimit) {
                    storage = storage.prepareFlush();
                    storage.currentStorage.put(record.getKey(), record);
                    flushTask(size);
                }
            }
        } else {
            storage.currentStorage.put(record.getKey(), record);
        }
    }

    private void flushTask(int size) {
        executors.execute(() -> {
            synchronized (this) {
                memoryConsumption.set(size);
                SSTable table;
                try {
                    table = flush();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                storage = storage.afterFlush(table);
                compactTask();
            }
        });
    }

    private SSTable flush() throws IOException {
        String newFileName = getNewFileName();
        return writeStorage(storage.storageToWrite, config.dir.resolve(newFileName));
    }

    private SSTable writeStorage(SortedMap<ByteBuffer, Record> storage, Path filePath) throws IOException {
        SSTable.write(storage.values().iterator(), filePath);
        return SSTable.loadFromFile(filePath);
    }

    @Override
    public void close() throws IOException {
        executors.shutdown();
        try {
            if (!executors.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                throw new IllegalStateException("Can't await termination on close");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }

        synchronized (this) {
            storage = storage.prepareFlush();
            SSTable table = flush();
            storage = storage.afterFlush(table);
            for (SSTable ssTable : storage.tables) {
                ssTable.close();
            }
            storage = null;
        }
    }

    private void compactTask() {
        synchronized (this) {
            if (this.storage.tables.size() < config.maxTables) {
                return;
            }
            compaction();
        }
    }

    @GuardedBy("LsmDAO.this")
    private void compaction() {
        SSTable compactFile = null;
        try {
            compactFile = SSTable.compact(config.dir, this.range(null, null), getNewFileName());
            Storage s = this.storage;
            this.storage = storage.afterCompaction(compactFile);
            for (SSTable t : s.tables) {
                t.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Can't compact", e);
        }
    }

    @Override
    public void compact() {
        synchronized (this) {
            compaction();
        }
    }

    /**
     * Merge iterators into one iterator.
     */
    public static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        return new MergingIterator(iterators);
    }
}
