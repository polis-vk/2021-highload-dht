package ru.mail.polis.lsm.artem_drozdov;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class LsmDAO implements DAO {

    private static final Logger LOGGER = LoggerFactory.getLogger(LsmDAO.class);
    private final AtomicReference<Storage> storage;

    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final DAOConfig config;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public LsmDAO(DAOConfig config) throws IOException {
        this.config = config;
        this.storage = new AtomicReference<>(Storage.init(SSTable.loadFromDir(config.dir)));
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
        Storage storageSnap = this.storage.get();

        Iterator<Record> sstableRanges = sstableRanges(storageSnap, fromKey, toKey);
        Iterator<Record> memoryRange = storageSnap.iterator(fromKey, toKey);

        Iterator<Record> iterator = new RecordMergingIterator(
            new PeekingIterator<>(sstableRanges), new PeekingIterator<>(memoryRange));

        return new TombstoneFilteringIterator(iterator);
    }

    @Override
    public Iterator<Record> rangeWithoutTombstoneFiltering(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        Storage storageSnap = this.storage.get();

        Iterator<Record> sstableRanges = sstableRanges(storageSnap, fromKey, toKey);
        Iterator<Record> memoryRange = storageSnap.iterator(fromKey, toKey);

        return new RecordMergingIterator(
            new PeekingIterator<>(sstableRanges), new PeekingIterator<>(memoryRange));
    }

    @Override
    public void upsert(Record record) {
        Storage storageSnap = this.storage.get();
        long consumption = storageSnap.currentMemTable.putAndGetSize(record);

        if (consumption > config.memoryLimit) {

            boolean success = this.storage.compareAndSet(storageSnap, storageSnap.prepareFlush());
            if (!success) {
                // another thread updated the storage
                return;
            }

            if (storageSnap.memTablesToFlush.isEmpty()) {
                // another thread already works on these storages
                return;
            }

            executor.execute(() -> {
                try {
                    LOGGER.info("Start flush");
                    Storage newStorage = doFlush();
                    if (storageSnap.ssTables.size() > config.maxTables) {
                        performCompact(newStorage);
                    }
                    LOGGER.info("Flush finished");
                } catch (IOException e) {
                    LOGGER.error("Fail to flush", e);
                }
            });
        }
    }

    private Storage doFlush() throws IOException {
        while (true) {
            Storage storageToFlush = this.storage.get();
            List<MemTable> storageToWrite = storageToFlush.memTablesToFlush;
            if (storageToWrite.isEmpty()) {
                return storageToFlush;
            }

            SSTable newTable = flushAll(storageToFlush);

            this.storage.updateAndGet(currentValue -> currentValue.afterFlush(storageToWrite, newTable));
        }
    }

    @Override
    public void compact() {
        executor.execute(() -> {
            try {
                performCompact(doFlush());
            } catch (IOException e) {
                LOGGER.error("can't compact", e);
            }
        });
    }

    private void performCompact(Storage storage) throws IOException {
        LOGGER.info("compact started");
        SSTable result = SSTable.compact(config.dir, range(null, null));

        this.storage.updateAndGet(currentValue -> currentValue.afterCompaction(storage.memTablesToFlush, result));
        LOGGER.info("compact finished");
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

        flushAll(storage.get().prepareFlush());
    }

    private SSTable flushAll(Storage storage) throws IOException {
        Path dir = config.dir;
        Path file = dir.resolve(SSTable.SSTABLE_FILE_PREFIX + storage.ssTables.size());

        return SSTable.write(storage.flushIterator(), file);
    }

    private Iterator<Record> sstableRanges(Storage storage, @Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(storage.ssTables.size());
        for (SSTable ssTable : storage.ssTables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return merge(iterators);
    }

    private static class Storage {

        private final MemTable currentMemTable;
        private final List<MemTable> memTablesToFlush;
        private final List<SSTable> ssTables;

        private Storage(
            MemTable currentMemTable,
            List<MemTable> memTablesToFlush,
            List<SSTable> ssTables) {
            this.currentMemTable = currentMemTable;
            this.memTablesToFlush = memTablesToFlush;
            this.ssTables = ssTables;
        }

        public static Storage init(List<SSTable> tables) {
            return new Storage(
                new MemTable(),
                Collections.emptyList(),
                tables
            );
        }

        public Storage prepareFlush() {
            List<MemTable> storagesToWrite = new ArrayList<>(this.memTablesToFlush.size() + 1);
            storagesToWrite.addAll(this.memTablesToFlush);
            storagesToWrite.add(currentMemTable);
            return new Storage(new MemTable(), storagesToWrite, ssTables);
        }

        // it is assumed that memTablesToFlush starts with writtenStorages
        public Storage afterFlush(List<MemTable> writtenStorage, SSTable newTable) {
            List<SSTable> newTables = new ArrayList<>(ssTables.size() + 1);
            newTables.addAll(ssTables);
            newTables.add(newTable);

            List<MemTable> newMemTablesToFlush = memTablesToFlush.subList(
                writtenStorage.size(), memTablesToFlush.size());
            return new Storage(currentMemTable, new ArrayList<>(newMemTablesToFlush), newTables);
        }

        // it is assumed that memTablesToFlush starts with writtenStorages
        public Storage afterCompaction(List<MemTable> writtenStorage, SSTable ssTable) {
            List<MemTable> newMemTablesToFlush = memTablesToFlush.subList(
                writtenStorage.size(),
                memTablesToFlush.size());
            return new Storage(
                currentMemTable,
                new ArrayList<>(newMemTablesToFlush),
                Collections.singletonList(ssTable));
        }

        public Iterator<Record> iterator(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
            List<Iterator<Record>> iterators = new ArrayList<>();

            // order is mater - older data first
            for (SSTable table : ssTables) {
                iterators.add(table.range(fromKey, toKey));
            }
            for (MemTable memTable : memTablesToFlush) {
                iterators.add(memTable.range(fromKey, toKey));
            }
            iterators.add(currentMemTable.range(fromKey, toKey));

            return merge(iterators);
        }

        public Iterator<Record> flushIterator() {
            List<Iterator<Record>> iterators = new ArrayList<>();
            // order is mater - older data first
            for (MemTable memTable : memTablesToFlush) {
                iterators.add(memTable.range(null, null));
            }
            return merge(iterators);
        }
    }
}
