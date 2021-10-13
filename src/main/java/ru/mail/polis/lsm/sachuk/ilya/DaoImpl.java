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
import javax.annotation.concurrent.GuardedBy;
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
    private Storage storage;

    private final DAOConfig config;

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final ExecutorService flushExecutor = Executors.newSingleThreadExecutor();
    private final AtomicInteger memoryConsumption = new AtomicInteger();

    /**
     * Constructor that initialize path and restore storage.
     *
     * @param config is config.
     * @throws IOException is thrown when an I/O error occurs.
     */
    public DaoImpl(DAOConfig config) throws IOException {
        this.config = config;
        Path dirPath = config.dir;

        storage = Storage.init(SSTable.loadFromDir(dirPath));
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        Storage storage = this.storage;

        logger.info(String.format("size sstable: %s%n", storage.ssTables.size()));

        Iterator<Record> ssTableRanges = ssTableRanges(storage, fromKey, toKey);
        Iterator<Record> memoryRange = map(storage.currentStorage, fromKey, toKey).values().iterator();
        Iterator<Record> tmpMemoryRange = map(storage.storageToWrite, fromKey, toKey).values().iterator();

        Iterator<Record> memory = mergeTwo(
                new PeekingIterator<>(memoryRange),
                new PeekingIterator<>(tmpMemoryRange)
        );
        Iterator<Record> mergedIterators = mergeTwo(
                new PeekingIterator<>(ssTableRanges),
                new PeekingIterator<>(memory)
        );

        return new TombstoneFilteringIterator(mergedIterators);
    }

    @Override
    public void upsert(Record record) {
        int consumption = memoryConsumption.addAndGet(sizeOf(record));
        if (consumption > config.memoryLimit) {
            synchronized (this) {
                if (memoryConsumption.get() > config.memoryLimit) {
                    storage = storage.prepareFlush();

                    int prev = memoryConsumption.getAndSet(sizeOf(record));

                    flushExecutor.execute(() -> {
                        SSTable ssTable = prepareAndFlush(prev);
                        storage = storage.afterFlush(ssTable);
                    });
//                        if (needCompact()) {
//                            compact();
//                        }
                }
            }
        }

        storage.currentStorage.put(record.getKey(), record);
    }

    private boolean needCompact() {
        return storage.ssTables.size() > config.maxTables;
    }

    @Override
    public void compact() {
        executorService.execute(() -> {
            synchronized (DaoImpl.class) {
                try {
                    if (!needCompact()) {
                        return;
                    }
                    logger.info("comcapt started");

                    SSTable result = SSTable.compact(config.dir, ssTableRanges(storage, null, null));
                    storage = storage.afterCompaction(result);

                    logger.info("compact finished");
                } catch (IOException e) {
                    throw new UncheckedIOException("Can't compact", e);
                } catch (Exception e) {
                    logger.error("Can't run compaction", e);
                }
            }
        });
    }

    @Override
    public void close() throws IOException {

        flushExecutor.shutdown();
        try {
            if (!flushExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                throw new IllegalStateException("Can't await for termination");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                throw new IllegalStateException("Can't await for termination");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }

        synchronized (this) {
            if (memoryConsumption.get() > 0) {
                storage = storage.prepareFlush();
                flush();
            }
            storage = null;

        }
    }

    private Map<ByteBuffer, Record> map(
            NavigableMap<ByteBuffer, Record> memoryStorage,
            @Nullable ByteBuffer fromKey,
            @Nullable ByteBuffer toKey
    ) {

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

    private SSTable prepareAndFlush(int prevConsumption) {
        try {
            return flush();
        } catch (IOException e) {
            memoryConsumption.addAndGet(prevConsumption);
            throw new UncheckedIOException(e);
        }
    }

    private SSTable flush() throws IOException {
        Path dir = config.dir;
        Path file = dir.resolve(SSTable.SAVE_FILE_PREFIX + storage.ssTables.size());

        return SSTable.save(storage.storageToWrite.values().iterator(), file);
    }

    private Iterator<Record> ssTableRanges(Storage storage, @Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(storage.ssTables.size());

        for (SSTable ssTable : storage.ssTables) {
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

    private static class Storage {
        private static final NavigableMap<ByteBuffer, Record> EMPTY_STORAGE = Collections.emptyNavigableMap();

        public final NavigableMap<ByteBuffer, Record> currentStorage;
        public final NavigableMap<ByteBuffer, Record> storageToWrite;
        public final List<SSTable> ssTables;

        private Storage(
                NavigableMap<ByteBuffer,
                        Record> currentStorage, NavigableMap<ByteBuffer,
                Record> storageToWrite, List<SSTable>
                        ssTables
        ) {
            this.currentStorage = currentStorage;
            this.storageToWrite = storageToWrite;
            this.ssTables = ssTables;
        }

        public static Storage init(List<SSTable> ssTables) {
            return new Storage(new ConcurrentSkipListMap<>(), EMPTY_STORAGE, ssTables);
        }

        public Storage prepareFlush() {
            return new Storage(new ConcurrentSkipListMap<>(), currentStorage, ssTables);
        }

        public Storage afterFlush(SSTable newTable) {
            List<SSTable> newTables = new ArrayList<>(ssTables.size() + 1);
            newTables.addAll(ssTables);
            newTables.add(newTable);

            return new Storage(currentStorage, EMPTY_STORAGE, newTables);
        }

        public Storage afterCompaction(SSTable ssTable) {
            List<SSTable> tables = Collections.singletonList(ssTable);
            return new Storage(currentStorage, EMPTY_STORAGE, tables);
        }
    }
}

