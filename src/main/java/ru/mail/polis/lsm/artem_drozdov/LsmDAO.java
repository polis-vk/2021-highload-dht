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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class LsmDAO implements DAO {

    private static final Logger LOG = LoggerFactory.getLogger(LsmDAO.class);

    private final ConcurrentLinkedDeque<NavigableMap<ByteBuffer, Record>> tablesForFlush = new ConcurrentLinkedDeque<>();
    private volatile Future<?> currentFlush;

    private final AtomicBoolean storageChanging = new AtomicBoolean();

    private final ExecutorService flushExecutor = Executors.newSingleThreadExecutor();

    private NavigableMap<ByteBuffer, Record> memoryStorage = newStorage();
    private final ConcurrentLinkedDeque<SSTable> tables = new ConcurrentLinkedDeque<>();

    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final DAOConfig config;

    private final AtomicInteger memoryConsumption = new AtomicInteger();

    public LsmDAO(DAOConfig config) throws IOException {
        this.config = config;
        List<SSTable> ssTables = SSTable.loadFromDir(config.dir);
        tables.addAll(ssTables);
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        Iterator<Record> sstableRanges = sstableRanges(fromKey, toKey);
        Iterator<Record> memoryRange = map(memoryStorage, fromKey, toKey).values().iterator();
        Iterator<Record> iterator = mergeTwo(new PeekingIterator(sstableRanges), new PeekingIterator(memoryRange));
        List<Iterator<Record>> tablesForFlushRange = tablesForFlush.stream()
                .map(table -> map(table, fromKey, toKey).values().iterator())
                .collect(Collectors.toList());
        Iterator<Record> tableForFlushRange = merge(tablesForFlushRange);
        Iterator<Record> finalIterator = mergeTwo(new PeekingIterator(tableForFlushRange), new PeekingIterator(iterator));
        return filterTombstones(finalIterator);
    }

    @Override
    public void upsert(Record record) {
        while (storageChanging.get()) {
            synchronized (this) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    LOG.warn("Wait was interrupted", e);
                    Thread.currentThread().interrupt();
                }
            }
        }
        int recordSize = sizeOf(record);
        if (memoryConsumption.addAndGet(recordSize) > config.memoryLimit) {
            synchronized (this) {
                if (memoryConsumption.get() > config.memoryLimit) {
                    int prev = memoryConsumption.getAndSet(recordSize);

                    waitForFlushFinished();

                    storageChanging.set(true);
                    try {
                        tablesForFlush.add(memoryStorage);
                        currentFlush = flushExecutor.submit(() -> {
                            NavigableMap<ByteBuffer, Record> snapshotToFlush = tablesForFlush.poll();
                            if (snapshotToFlush != null) {
                                doSnapshotFlush(prev, snapshotToFlush.values().iterator());
                            }
                        });
                        memoryStorage = new ConcurrentSkipListMap<>();
                    } finally {
                        notifyAll();
                        storageChanging.set(false);
                    }
                }
            }
        }
        memoryStorage.put(record.getKey(), record);
    }

    private void doSnapshotFlush(int prev, Iterator<Record> snapshot) {
        try {
            flush(snapshot);
        } catch (IOException e) {
            memoryConsumption.addAndGet(prev);
            throw new UncheckedIOException(e);
        }
    }

    private void waitForFlushFinished() {
        if (currentFlush != null && !currentFlush.isDone()) {
            try {
                currentFlush.get(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException | TimeoutException e) {
                LOG.warn("Failed to get future", e);
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
                throw new RuntimeException(e);
            }
            tables.clear();
            tables.add(table);
            memoryStorage = newStorage();
        }
    }

    private NavigableMap<ByteBuffer, Record> newStorage() {
        return new ConcurrentSkipListMap<>();
    }

    private int sizeOf(Record record) {
        int keySize = Integer.BYTES + record.getKeySize();
        int valueSize = Integer.BYTES + record.getValueSize();
        return keySize + valueSize;
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            waitForFlushFinished();
            flush(memoryStorage.values().iterator());
        }
    }

    private void flush(Iterator<Record> iterator) throws IOException {
        Path dir = config.dir;
        Path file = dir.resolve(SSTable.SSTABLE_FILE_PREFIX + tables.size());
        SSTable ssTable = SSTable.write(iterator, file);
        tables.add(ssTable);
    }

    private Iterator<Record> sstableRanges(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(tables.size());
        for (SSTable ssTable : tables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return merge(iterators);
    }

    private SortedMap<ByteBuffer, Record> map(NavigableMap<ByteBuffer, Record> storage, @Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return storage;
        }
        if (fromKey == null) {
            return storage.headMap(toKey);
        }
        if (toKey == null) {
            return storage.tailMap(fromKey);
        }
        return storage.subMap(fromKey, toKey);
    }

    public static Iterator<Record> merge(List<Iterator<Record>> iterators) {
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
        return new MergeIterator(left, right);
    }

    private static Iterator<Record> filterTombstones(Iterator<Record> iterator) {
        return new TombstoneFilterIterator(new PeekingIterator(iterator));
    }

    static class PeekingIterator implements Iterator<Record> {

        private Record current;

        private final Iterator<Record> delegate;

        public PeekingIterator(Iterator<Record> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return current != null || delegate.hasNext();
        }

        @Override
        public Record next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Record now = peek();
            current = null;
            return now;
        }

        public Record peek() {
            if (current != null) {
                return current;
            }

            if (!delegate.hasNext()) {
                return null;
            }

            current = delegate.next();
            return current;
        }

    }
}
