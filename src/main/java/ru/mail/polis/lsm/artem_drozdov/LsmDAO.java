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
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class LsmDAO implements DAO {

    private static final Logger LOG = LoggerFactory.getLogger(LsmDAO.class);

    private final Semaphore semaphore = new Semaphore(1);
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
        Iterator<Record> memoryRange = map(fromKey, toKey).values().iterator();
        Iterator<Record> iterator = mergeTwo(new PeekingIterator(sstableRanges), new PeekingIterator(memoryRange));
        return filterTombstones(iterator);
    }

    @Override
    public void upsert(Record record) {
        int recordSize = sizeOf(record);
        if (memoryConsumption.addAndGet(recordSize) > config.memoryLimit) {
            synchronized (this) {
                if (memoryConsumption.get() > config.memoryLimit) {
                    try {
                        semaphore.acquire();
                        int prev = memoryConsumption.getAndSet(recordSize);
                        final Iterator<Record> snapshot = memoryStorage.values().iterator();
                        Future<?> future = flushExecutor.submit(() -> {
                            doSnapshotFlush(prev, snapshot);
                        });
                        LOG.info("Submitted flush task: {}", future);
                        memoryStorage = new ConcurrentSkipListMap<>();
                    } catch (InterruptedException e) {
                        semaphore.release();
                        Thread.currentThread().interrupt();
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
        } finally {
            semaphore.release();
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
        try {
            semaphore.acquire();
            flush(memoryStorage.values().iterator());
            semaphore.release();
        } catch (InterruptedException e) {
            semaphore.release();
            Thread.currentThread().interrupt();
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

    private SortedMap<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return memoryStorage;
        }
        if (fromKey == null) {
            return memoryStorage.headMap(toKey);
        }
        if (toKey == null) {
            return memoryStorage.tailMap(fromKey);
        }
        return memoryStorage.subMap(fromKey, toKey);
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
