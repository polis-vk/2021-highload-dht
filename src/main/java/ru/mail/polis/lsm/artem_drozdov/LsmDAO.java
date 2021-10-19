package ru.mail.polis.lsm.artem_drozdov;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class LsmDAO implements DAO {

    private static final Logger logger = LoggerFactory.getLogger(LsmDAO.class);

    private final AtomicReference<Storage> storage;

    private final DAOConfig config;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public LsmDAO(DAOConfig config) throws IOException {
        this.config = config;
        storage = new AtomicReference<>(Storage.init(SSTable.loadFromDir(config.dir)));
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        Storage storage = this.storage.get();

        Iterator<Record> sstableRanges = sstableRanges(storage, fromKey, toKey);
        Iterator<Record> memoryRange = storage.iterator(fromKey, toKey);
        Iterator<Record> iterator = mergeTwo(new PeekingIterator(sstableRanges), new PeekingIterator(memoryRange));
        return filterTombstones(iterator);
    }

    @Override
    public void upsert(Record record) {
        Storage storage = this.storage.get();
        long consumption = storage.currentMemTable.putAndGetSize(record);
        if (consumption > config.memoryLimit) {
            boolean success = this.storage.compareAndSet(storage, storage.prepareFlush());
            if (!success) {
                // another thread updated storage
                return;
            }

            if (!storage.memTablesToFlush.isEmpty()) {
                // another thread already works on those storages
                return;
            }

            executor.execute(() -> {
                try {
                    logger.info("Start flush");
                    Storage newStorage = doFlush();

                    if (storage.ssTables.size() > config.maxTables) {
                        performCompactNeed(newStorage);
                    }
                    logger.info("Flush finished");
                } catch (IOException e) {
                    logger.error("Fail to flush", e);
                    throw new UncheckedIOException(e);
                }
            });
        }
    }

    @Override
    public void compact() {
        executor.execute(() -> {
            try {
                performCompactNeed(doFlush());
            } catch (IOException e) {
                logger.error("Can't compact", e);
                throw new UncheckedIOException(e);
            }
        });
    }

    private Storage doFlush() throws IOException {
        while (true) {
            Storage storageToFlush = LsmDAO.this.storage.get();
            List<MemTable> storagesToWrite = storageToFlush.memTablesToFlush;
            if (storagesToWrite.isEmpty()) {
                return storageToFlush;
            }
            SSTable newTable = flushAll(storageToFlush);

            LsmDAO.this.storage.updateAndGet(currentValue -> currentValue.afterFlush(storagesToWrite, newTable));
        }
    }

    private void performCompactNeed(Storage storage) throws IOException {
        logger.info("Compact started");

        SSTable result = SSTable.compact(config.dir, sstableRanges(storage, null, null));

        this.storage.updateAndGet(currentValue -> currentValue.afterCompaction(storage.memTablesToFlush, result));

        logger.info("Compact finished");
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

    private static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        if (iterators.isEmpty()) {
            return Collections.emptyIterator();
        }
        if (iterators.size() == 1) {
            return iterators.get(0);
        }
        if (iterators.size() == 2) {
            return mergeTwo(new PeekingIterator(iterators.get(0)), new PeekingIterator(iterators.get(1)));
        }
        Iterator<Record> left = merge(iterators.subList(0, iterators.size() / 2));
        Iterator<Record> right = merge(iterators.subList(iterators.size() / 2, iterators.size()));
        return mergeTwo(new PeekingIterator(left), new PeekingIterator(right));
    }

    private static Iterator<Record> mergeTwo(PeekingIterator left, PeekingIterator right) {
        return new MergeTwoIterator(left, right);
    }

    private static Iterator<Record> filterTombstones(Iterator<Record> iterator) {
        PeekingIterator delegate = new PeekingIterator(iterator);
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                for (; ; ) {
                    Record peek = delegate.peek();
                    if (peek == null) {
                        return false;
                    }
                    if (!peek.isTombstone()) {
                        return true;
                    }

                    delegate.next();
                }
            }

            @Override
            public Record next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("No elements");
                }
                return delegate.next();
            }
        };
    }

    private static class Storage {
        final MemTable currentMemTable;
        final List<MemTable> memTablesToFlush;
        final List<SSTable> ssTables;

        private Storage(MemTable currentMemTable, List<MemTable> memTablesToFlush, List<SSTable> ssTables) {
            this.currentMemTable = currentMemTable;
            this.memTablesToFlush = memTablesToFlush;
            this.ssTables = ssTables;
        }

        public static Storage init(List<SSTable> tables) {
            return new Storage(new MemTable(), Collections.emptyList(), tables);
        }

        public Storage prepareFlush() {
            List<MemTable> storagesToWrite = new ArrayList<>(this.memTablesToFlush.size() + 1);
            storagesToWrite.addAll(this.memTablesToFlush);
            storagesToWrite.add(currentMemTable);
            return new Storage(new MemTable(), storagesToWrite, ssTables);
        }

        // It is assumed that memTablesToFlush starts with writtenStorages
        public Storage afterFlush(List<MemTable> writtenStorages, SSTable newTable) {
            List<SSTable> newTables = new ArrayList<>(ssTables.size() + 1);
            newTables.addAll(ssTables);
            newTables.add(newTable);

            List<MemTable> newMemTablesToFlush = memTablesToFlush.subList(writtenStorages.size(), memTablesToFlush.size());
            return new Storage(currentMemTable, new ArrayList<>(newMemTablesToFlush), newTables);
        }

        // It is assumed that memTablesToFlush starts with writtenStorages
        public Storage afterCompaction(List<MemTable> writtenStorages, SSTable ssTable) {
            List<MemTable> newMemTablesToFlush = memTablesToFlush.subList(writtenStorages.size(), memTablesToFlush.size());
            return new Storage(currentMemTable, new ArrayList<>(newMemTablesToFlush), Collections.singletonList(ssTable));
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

            //Iterator<Record> memory = mergeTwo(new PeekingIterator(memoryRange), new PeekingIterator(tmpMemoryRange));
            return merge(iterators);
        }

        public Iterator<Record> flushIterator() {
            List<Iterator<Record>> iterators = new ArrayList<>();

            for (MemTable memTable : memTablesToFlush) {
                iterators.add(memTable.range(null, null));
            }
            return merge(iterators);
        }
    }
}
