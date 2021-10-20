package ru.mail.polis.lsm.igorsamokhin;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class LsmDAO implements DAO {
    @SuppressWarnings("PMD.AvoidUsingVolatile")//Storage is final class
    private final AtomicReference<Storage> storage = new AtomicReference<>();
    private final DAOConfig config;

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
        storage.set(Storage.init(ssTables));
    }

    private String getNewFileName() {
        int size = fileN.getAndIncrement();
        return FileUtils.formatFileName(null, size, null);
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return this.storage.get().range(fromKey, toKey);
    }

    /**
     * Throws RuntimeException when it is too many requests on upsert.
     */
    @SuppressWarnings("PMD.AvoidUncheckedExceptionsInSignatures") //I need it because it is hard to remember
    @Override
    public void upsert(Record record) throws RuntimeException {
        Storage currentStorage = this.storage.get();
        if (currentStorage.memTablesToFlush.size() > config.maxTables) {
            throw new RuntimeException("To many requests on flush");
        }
        int consumption = currentStorage.currentMemTable.putAndGetSize(record);

        if (consumption > config.memoryLimit) {
            boolean success = this.storage.compareAndSet(currentStorage, currentStorage.prepareFlush());
            if (!success) {
                //another thread updated the storage
                return;
            }

            if (!currentStorage.memTablesToFlush.isEmpty()) {
                //another thread already works on those storages
                return;
            }

            executors.execute(() -> {
                try {
                    Storage newStorage = doFlush();
                    if (currentStorage.ssTables.size() > config.maxTables) {
                        performCompactIfNeed(newStorage);
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }

    private void performCompactIfNeed(Storage storage) throws IOException {
        SSTable result = SSTable.compact(config.dir, storage.compactIterator(), getNewFileName());

        this.storage.updateAndGet(curr -> curr.afterCompaction(storage.memTablesToFlush, result));
    }

    private Storage doFlush() throws IOException {
        while (true) {
            Storage storageToFlush = this.storage.get();
            List<MemTable> storagesToWrite = storageToFlush.memTablesToFlush;
            if (storagesToWrite.isEmpty()) {
                return storageToFlush;
            }
            SSTable newTable = flushAll(storageToFlush.flushIterator());
            this.storage.updateAndGet(currentValue -> currentValue.afterFlush(storagesToWrite, newTable));
        }
    }

    private SSTable flushAll(Iterator<Record> iterator) throws IOException {
        String newFileName = getNewFileName();
        return writeStorage(iterator, config.dir.resolve(newFileName));
    }

    private SSTable writeStorage(Iterator<Record> iterator, Path filePath) throws IOException {
        SSTable.write(iterator, filePath);
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
            Storage old = this.storage.getAndSet(null);
            SSTable table = flushAll(old.currentMemTable.range(null, null));
            table.close();
            for (SSTable ssTable : old.ssTables) {
                ssTable.close();
            }
        }
    }

    @Override
    public void compact() {
        executors.execute(() -> {
            synchronized (this) {
                try {
                    performCompactIfNeed(doFlush());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        });
    }
}
