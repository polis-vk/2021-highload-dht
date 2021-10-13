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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LsmDAO implements DAO {

    private static final Logger LOGGER = LoggerFactory.getLogger(LsmDAO.class);
    private Storage storage;

    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final DAOConfig config;

    private final AtomicInteger memoryConsumption = new AtomicInteger();

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private CompletableFuture<Void> cfFlush;

    public LsmDAO(DAOConfig config) throws IOException {
        this.config = config;
        this.storage = Storage.init(SSTable.loadFromDir(config.dir));
    }

    private static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        final int size = iterators.size();
        if (size == 0) {
            return Collections.emptyIterator();
        } else if (size == 1) {
            return iterators.get(0);
        } else if (size == 2) {
            return new RecordMergingIterator(
                new PeekingIterator<>(iterators.get(0)),
                new PeekingIterator<>(iterators.get(1)));
        }
        Iterator<Record> left = merge(iterators.subList(0, size / 2));
        Iterator<Record> right = merge(iterators.subList(size / 2, size));
        return new RecordMergingIterator(new PeekingIterator<>(left), new PeekingIterator<>(right));
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        Iterator<Record> sstableRanges = sstableRanges(storage, fromKey, toKey);
        Iterator<Record> memoryRange = map(storage.currentStorage, fromKey, toKey).values().iterator();
        Iterator<Record> tmpMemoryRange = map(storage.storageToWrite, fromKey, toKey).values().iterator();

        Iterator<Record> memory = new RecordMergingIterator(
            new PeekingIterator<>(memoryRange), new PeekingIterator<>(tmpMemoryRange));
        Iterator<Record> iterator = new RecordMergingIterator(
            new PeekingIterator<>(sstableRanges), new PeekingIterator<>(memory));
        return new TombstoneFilteringIterator(iterator);
    }

    @Override
    public void upsert(Record record) {
        int consumption = memoryConsumption.get() + sizeOf(record);
        if (memoryConsumption.addAndGet(sizeOf(record)) > config.memoryLimit) {
            LOGGER.debug("Going to flush {}", consumption);
            synchronized (this) {
                if (memoryConsumption.get() > config.memoryLimit) {
                    waitingCompleteFlush();

                    int prev = memoryConsumption.getAndSet(sizeOf(record));
                    storage = storage.prepareFlush();

                    cfFlush = CompletableFuture.runAsync(() -> {
                        try {
                            LOGGER.debug("Start flush");
                            SSTable ssTable = flush();
                            storage = storage.afterFlush(ssTable);
                            LOGGER.debug("Flush finished");
                        } catch (IOException e) {
                            memoryConsumption.addAndGet(prev);
                            throw new UncheckedIOException(e);
                        }
                    }, executor);
                } else {
                    LOGGER.debug("Concurrent flush");
                }
            }

        }
        storage.currentStorage.put(record.getKey(), record);
    }

    private void waitingCompleteFlush() {
        if (cfFlush == null) {
            return;
        }

        try {
            cfFlush.get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("error get completable future", e);
        }
    }

    @Override
    public void compact() {
        synchronized (this) {
            LOGGER.info("compact started");
            final SSTable table;
            try {
                table = SSTable.compact(config.dir, range(null, null));
            } catch (IOException e) {
                throw new UncheckedIOException("Can't compact", e);
            }
            storage = storage.afterCompaction(table);
            LOGGER.info("compact finished");
        }
    }

    private int sizeOf(Record record) {
        return SSTable.sizeOf(record);
    }

    @Override
    public void close() throws IOException {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                throw new IllegalStateException("Can't await for termination");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        waitingCompleteFlush();
        storage = storage.prepareFlush();
        flush();
        storage = null;

    }

    private SSTable flush() throws IOException {
        Path dir = config.dir;
        Path file = dir.resolve(SSTable.SSTABLE_FILE_PREFIX + storage.tables.size());

        return SSTable.write(storage.storageToWrite.values().iterator(), file);
    }

    private Iterator<Record> sstableRanges(Storage storage, @Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(storage.tables.size());
        for (SSTable ssTable : storage.tables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return merge(iterators);
    }

    private NavigableMap<ByteBuffer, Record> map(
        NavigableMap<ByteBuffer, Record> storage,
        @Nullable ByteBuffer fromKey,
        @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return storage;
        } else if (fromKey == null) {
            return storage.headMap(toKey, false);
        } else if (toKey == null) {
            return storage.tailMap(fromKey, true);
        } else {
            return storage.subMap(fromKey, true, toKey, false);
        }
    }

    private static class Storage {
        private final static NavigableMap<ByteBuffer, Record> EMPTY_STORAGE =
            Collections.emptyNavigableMap();

        private final NavigableMap<ByteBuffer, Record> currentStorage;
        private final NavigableMap<ByteBuffer, Record> storageToWrite;

        private final List<SSTable> tables;

        private Storage(
            NavigableMap<ByteBuffer, Record> currentStorage,
            NavigableMap<ByteBuffer, Record> storageToWrite,
            List<SSTable> tables) {
            this.currentStorage = currentStorage;
            this.storageToWrite = storageToWrite;
            this.tables = tables;
        }

        public static Storage init(List<SSTable> tables) {
            return new Storage(
                new ConcurrentSkipListMap<>(),
                EMPTY_STORAGE,
                tables
            );
        }

        public Storage prepareFlush() {
            return new Storage(new ConcurrentSkipListMap<>(), currentStorage, tables);
        }

        public Storage afterFlush(SSTable newTable) {
            List<SSTable> newTables = new ArrayList<>(tables);
            newTables.add(newTable);
            return new Storage(currentStorage, EMPTY_STORAGE, newTables);
        }

        public Storage afterCompaction(SSTable ssTable) {
            List<SSTable> newTables = Collections.singletonList(ssTable);
            return new Storage(currentStorage, EMPTY_STORAGE, newTables);
        }
    }
}
