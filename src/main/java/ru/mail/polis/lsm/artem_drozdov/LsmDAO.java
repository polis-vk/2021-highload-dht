package ru.mail.polis.lsm.artem_drozdov;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LsmDAO implements DAO {

    private final long flushMemoryThreshold;

    private final ExecutorService executor = Executors.newWorkStealingPool();
    private final NavigableMap<Integer, CompletableFuture<?>> runningFlushes = new ConcurrentSkipListMap<>();
    private final AtomicInteger currentIndex = new AtomicInteger(0);
    private volatile Storage storage;

    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final DAOConfig config;

    public LsmDAO(DAOConfig config) throws IOException {
        this.config = config;
        this.flushMemoryThreshold = (long) (config.memoryLimit * 2.5);
        List<SSTable> ssTables = SSTable.loadFromDir(config.dir);
        storage = new Storage(ssTables);
        currentIndex.set(storage.tablesSize());
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return storage.range(fromKey, toKey);
    }

    @Override
    public void upsert(Record record) {
        if (storage.getCurrentSize() > config.memoryLimit) {
            while (Runtime.getRuntime().freeMemory() < this.flushMemoryThreshold) {
                Map.Entry<Integer, CompletableFuture<?>> flushToWait = runningFlushes.firstEntry();
                if (flushToWait == null) {
                    break;
                }
                flushToWait.getValue().join();
            }

            synchronized (this) {
                if (storage.getCurrentSize() > config.memoryLimit) {
                    final int flushIndex = currentIndex.getAndIncrement();
                    storage = storage.willStartFlush(flushIndex);
                    CompletableFuture<?> future = CompletableFuture.runAsync(() -> flush(flushIndex, storage),
                            executor
                    );
                    runningFlushes.put(flushIndex, future);
                }
            }
        }
        storage.put(record);
    }

    @Override
    public void compact() {
        synchronized (this) {
            final SSTable table;
            try {
                table = SSTable.compact(config.dir, range(null, null));
                storage = new Storage(Collections.singletonList(table));
                currentIndex.set(1);
            } catch (IOException e) {
                throw new UncheckedIOException("Can't compact", e);
            }
        }
    }

    public static int sizeOf(Record record) {
        return SSTable.sizeOf(record);
    }

    @Override
    public void close() throws IOException {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.MINUTES)) {
                throw new IllegalStateException("Can't wait for termination");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }

        synchronized (this) {
            final int flushIndex = currentIndex.getAndIncrement();
            storage = storage.willStartFlush(flushIndex);
            flush(flushIndex, storage);
            storage = null;
        }
    }

    private void flush(int index, Storage storageToFlush) {
        try {
            Path dir = config.dir;
            Path file = dir.resolve(SSTable.SSTABLE_FILE_PREFIX + index);
            SSTable ssTable = SSTable.write(
                    storageToFlush.getFlushingStorages().get(index).getMemory().values().iterator(),
                    file
            );

            CompletableFuture<?> previousFlush = runningFlushes.get(index - 1);
            if (previousFlush != null) {
                previousFlush.join();
            }

            synchronized (this) {
                storage = storage.doneFlush(index, ssTable);
            }
        } catch (IOException e) {
            synchronized (this) {
                storage = storage.restoreMemory(index);
            }
        } finally {
            runningFlushes.remove(index);
        }
    }
}
