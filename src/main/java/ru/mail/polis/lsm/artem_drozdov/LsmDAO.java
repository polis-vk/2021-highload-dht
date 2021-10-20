package ru.mail.polis.lsm.artem_drozdov;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import java.util.concurrent.atomic.AtomicReference;

public class LsmDAO implements DAO {

    private static final Logger LOGGER = LoggerFactory.getLogger(LsmDAO.class);

    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final DAOConfig config;

    private final AtomicReference<MemStorage> memoryStorage;

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    public LsmDAO(DAOConfig config) throws IOException {
        this.config = config;
        this.memoryStorage = new AtomicReference<>(
                MemStorage.init(SSTable.loadFromDir(config.dir))
        );
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {

        MemStorage memStorage = this.memoryStorage.get();

        Iterator<Record> sstableRanges = memStorage.ssTableIterator(fromKey, toKey);
        Iterator<Record> memoryRanges = memStorage.iterator(fromKey, toKey);

        Iterator<Record> iterator =
                new RecordMergingIterator(
                        new PeekingIterator<>(sstableRanges),
                        new PeekingIterator<>(memoryRanges));

        return new TombstoneFilteringIterator(iterator);
    }

    @Override
    public void upsert(Record record) {
        MemStorage memStorage = memoryStorage.get();

        long consumption = memStorage.currentMemTable.putAndGetSize(record);
        if (consumption > config.memoryLimit) {

            boolean success = memoryStorage.compareAndSet(memStorage, memStorage.prepareFlush());
            if (!success) {
                // another thread updated the storage
                return;
            }

            if (!memStorage.memTablesToWrite.isEmpty()) {
                // another thread already works on those storages
                return;
            }

            executorService.execute(() -> {
                try {
                    MemStorage newStorage = doFlush();
                    if (memStorage.ssTableList.size() > config.maxTables) {
                        performCompact(newStorage);
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }

    @Override
    public void compact() {
        executorService.execute(() -> {
            try {
                performCompact(doFlush());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private MemStorage doFlush() throws IOException {
        while (true) {
            MemStorage storageToFlush = LsmDAO.this.memoryStorage.get();
            List<MemTable> storagesToWrite = storageToFlush.memTablesToWrite;
            if (storagesToWrite.isEmpty()) {
                return storageToFlush;
            }

            SSTable newTable = flushAll(storageToFlush);

            LsmDAO.this.memoryStorage.updateAndGet(currentValue -> currentValue.afterFlush(storagesToWrite, newTable));
        }
    }

    private SSTable flushAll(MemStorage memStorage) throws IOException {
        Path dir = config.dir;
        Path file = dir.resolve(SSTable.SSTABLE_FILE_PREFIX + memStorage.ssTableList.size());

        return SSTable.write(memStorage.flushIterator(), file);
    }

    private void performCompact(MemStorage memStorage) {
        try {
            SSTable result = SSTable.compact(config.dir, memStorage.ssTableIterator(null, null));
            this.memoryStorage.updateAndGet(currentValue -> currentValue.afterCompaction(memStorage.memTablesToWrite, result));
        } catch (Exception e) {
            LOGGER.error("Can npt run compaction", e);
        }
    }

    @Override
    public void close() throws IOException {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                throw new IllegalStateException("Can not await for termination");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }

        MemStorage storage = this.memoryStorage.get().prepareFlush();
        flushAll(storage);
    }
}
