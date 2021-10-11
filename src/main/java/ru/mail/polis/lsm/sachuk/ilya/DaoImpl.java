package ru.mail.polis.lsm.sachuk.ilya;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.sachuk.ilya.iterators.MergeIterator;
import ru.mail.polis.lsm.sachuk.ilya.iterators.PeekingIterator;
import ru.mail.polis.lsm.sachuk.ilya.iterators.TombstoneFilteringIterator;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DaoImpl implements DAO {
    private final Logger logger = LoggerFactory.getLogger(DaoImpl.class);


    private final Path dirPath;
    private final DAOConfig config;
    private NavigableMap<ByteBuffer, Record> memoryStorage = new ConcurrentSkipListMap<>();
    private final List<SSTable> ssTables = new ArrayList<>();

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final AtomicInteger memoryConsumption = new AtomicInteger();

    /**
     * Constructor that initialize path and restore storage.
     *
     * @param config is config.
     * @throws IOException is thrown when an I/O error occurs.
     */
    public DaoImpl(DAOConfig config) throws IOException {
        this.config = config;
        this.dirPath = config.dir;

        ssTables.addAll(SSTable.loadFromDir(dirPath));
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        Iterator<Record> ssTableRanges = ssTableRanges(fromKey, toKey);

        Iterator<Record> memoryRange = map(fromKey, toKey).values().iterator();
        Iterator<Record> mergedIterators = mergeTwo(ssTableRanges, memoryRange);

        return new TombstoneFilteringIterator(mergedIterators);
    }

    @Override
    public void upsert(Record record) {
        if (memoryConsumption.addAndGet(sizeOf(record)) > config.memoryLimit) {
            synchronized (this) {
                if (memoryConsumption.get() > config.memoryLimit) {
                    NavigableMap<ByteBuffer, Record> old = memoryStorage;
                    memoryStorage = new ConcurrentSkipListMap<>();
                    int prev = memoryConsumption.getAndSet(sizeOf(record));

//                    try {
                    prepareAndFlush(old);
//                    } catch (IOException e) {
//                        memoryConsumption.addAndGet(prev);
//
//                        throw new UncheckedIOException(e);
//                    }
                }
            }
        }

        memoryStorage.put(record.getKey(), record);
    }

    private void prepareAndFlush(NavigableMap<ByteBuffer, Record> old) {
        try {
            flush(old);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void closeAndCompact() {
        synchronized (this) {
            final SSTable table;
            try {
                table = SSTable.compact(config.dir, range(null, null));
            } catch (IOException e) {
                throw new UncheckedIOException("Can't compact", e);
            }
            ssTables.clear();
            ssTables.add(table);
            memoryStorage = new ConcurrentSkipListMap<>();
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

        if (memoryConsumption.get() > 0) {
            flush(memoryStorage);
        }

        closeSSTables();
    }

    private void closeSSTables() throws IOException {
        for (SSTable ssTable : ssTables) {
            ssTable.close();
        }
    }

    private Map<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {

        if (fromKey == null && toKey == null) {
            return memoryStorage;
        } else if (fromKey == null) {
            return memoryStorage.headMap(toKey);
        } else if (toKey == null) {
            return memoryStorage.tailMap(fromKey);
        } else {
            return memoryStorage.subMap(fromKey, toKey);
        }
    }

    private void flush(NavigableMap<ByteBuffer, Record> storage) throws IOException {
        executorService.execute(() -> {
            try {
                Path dir = config.dir;
                Path file = dir.resolve(SSTable.SAVE_FILE_PREFIX + ssTables.size());

                SSTable ssTable = SSTable.save(storage.values().iterator(), file);

                ssTables.add(ssTable);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private Iterator<Record> ssTableRanges(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(ssTables.size());

        for (SSTable ssTable : ssTables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return merge(iterators);
    }

    private int sizeOf(Record record) {
        return record.getKey().remaining()
                + (record.isTombstone() ? 0 : record.getKey().remaining()) + Integer.BYTES * 2;
    }

    /**
     * Method that merge iterators and return iterator.
     *
     * @param iterators is list of iterators to merge
     * @return merged iterators
     */
    public static Iterator<Record> merge(List<Iterator<Record>> iterators) {

        if (iterators.isEmpty()) {
            return Collections.emptyIterator();
        } else if (iterators.size() == 1) {
            return iterators.get(0);
        } else if (iterators.size() == 2) {
            return mergeTwo(iterators.get(0), iterators.get(1));
        }

        Iterator<Record> left = merge(iterators.subList(0, iterators.size() / 2));
        Iterator<Record> right = merge(iterators.subList(iterators.size() / 2, iterators.size()));

        return mergeTwo(left, right);
    }

    private static Iterator<Record> mergeTwo(Iterator<Record> leftIterator, Iterator<Record> rightIterator) {
        return new MergeIterator(new PeekingIterator<>(leftIterator), new PeekingIterator<>(rightIterator));
    }
}

