package ru.mail.polis.lsm.artemdrozdov;

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
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class LsmDAO implements DAO {

    private volatile Future<?> flushFuture;
    private NavigableMap<ByteBuffer, Record> memoryStorage = newStorage();
    private NavigableMap<ByteBuffer, Record> memoryStorageToFlush = newStorage();
    private final ExecutorService flushExecutor = Executors.newSingleThreadExecutor();
    private final ConcurrentLinkedDeque<SSTable> tables = new ConcurrentLinkedDeque<>();
    Logger logger = LoggerFactory.getLogger(LsmDAO.class);

    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final DAOConfig config;

    private final AtomicInteger memoryConsumption = new AtomicInteger();

    /**
     *  Create LsmDAO from config.
     *
     * @param config - LamDAo config
     * @throws IOException - in case of io exception
     */
    public LsmDAO(DAOConfig config) throws IOException {
        this.config = config;
        List<SSTable> ssTables = SSTable.loadFromDir(config.dir);
        tables.addAll(ssTables);
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        synchronized (this) {
            Iterator<Record> sstableRanges = sstableRanges(fromKey, toKey);
            Iterator<Record> memoryRange = map(memoryStorage, fromKey, toKey).values().iterator();
            Iterator<Record> flushMemoryRange = map(memoryStorageToFlush, fromKey, toKey).values().iterator();
            Iterator<Record> memoryIterator = mergeTwo(new PeekingIterator(flushMemoryRange),
                    new PeekingIterator(memoryRange));
            Iterator<Record> iterator = mergeTwo(new PeekingIterator(sstableRanges),
                    new PeekingIterator(memoryIterator));
            return filterTombstones(iterator);
        }
    }

    @Override
    public void upsert(Record record) {
        if (memoryConsumption.addAndGet(sizeOf(record)) > config.memoryLimit) {
            synchronized (this) {
                if (memoryConsumption.get() > config.memoryLimit) {
                    if (flushFuture != null) {
                        try {
                            flushFuture.get();
                        } catch (InterruptedException | ExecutionException e) {
                            logger.error(e.getMessage());
                        }
                    }

                    int prev = memoryConsumption.getAndSet(sizeOf(record));
                    memoryStorageToFlush = new ConcurrentSkipListMap<>(memoryStorage);
                    memoryStorage = newStorage();

                    flushFuture = flushExecutor.submit(() -> {
                        try {
                            flush(memoryStorageToFlush);
                        } catch (IOException e) {
                            memoryConsumption.addAndGet(prev);
                            memoryStorage.putAll(memoryStorageToFlush);
                        }
                    });
                }
            }
        }

        memoryStorage.put(record.getKey(), record);
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
            memoryStorage = newStorage();
        }
    }

    private NavigableMap<ByteBuffer, Record> newStorage() {
        return new ConcurrentSkipListMap<>();
    }

    private int sizeOf(Record record) {
        return SSTable.sizeOf(record);
    }

    @Override
    public void close() throws IOException {
        if (flushFuture != null) {
            try {
                flushFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                logger.error(e.getMessage());
            }
        }

        flush(memoryStorage);
        flushExecutor.shutdown();
    }

    private void flush(NavigableMap<ByteBuffer, Record> data) throws IOException {
        Path dir = config.dir;
        Path file = dir.resolve(SSTable.SSTABLE_FILE_PREFIX + tables.size());

        SSTable ssTable = SSTable.write(data.values().iterator(), file);
        tables.add(ssTable);
    }

    private Iterator<Record> sstableRanges(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(tables.size());
        for (SSTable ssTable : tables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return merge(iterators);
    }

    private NavigableMap<ByteBuffer, Record> map(NavigableMap<ByteBuffer, Record> map, @Nullable ByteBuffer fromKey,
                                              @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return map;
        }
        if (fromKey == null) {
            return (NavigableMap<ByteBuffer, Record>) map.headMap(toKey);
        }
        if (toKey == null) {
            return (NavigableMap<ByteBuffer, Record>) map.tailMap(fromKey);
        }
        return (NavigableMap<ByteBuffer, Record>) map.subMap(fromKey, toKey);
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
        PeekingIterator delegate = new PeekingIterator(iterator);
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                for (;;) {
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

}
