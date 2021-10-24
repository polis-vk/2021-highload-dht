package ru.mail.polis.lsm.sachuk.ilya;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.ThreadUtils;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class DaoImpl implements DAO {
    private final Logger logger = LoggerFactory.getLogger(DaoImpl.class);

    @SuppressWarnings("PMD")
    private volatile Storage memoryStorage;

    private final DAOConfig config;

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final ExecutorService flushExecutor = Executors.newSingleThreadExecutor();
    private final AtomicInteger memoryConsumption = new AtomicInteger();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final AtomicReference<Future<?>> futureAtomicReference = new AtomicReference<>();

    /**
     * Constructor that initialize path and restore storage.
     *
     * @param config is config.
     * @throws IOException is thrown when an I/O error occurs.
     */
    public DaoImpl(DAOConfig config) throws IOException {
        this.config = config;
        Path dirPath = config.dir;

        memoryStorage = Storage.init(SSTable.loadFromDir(dirPath));
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        Storage storage = this.memoryStorage;

        checkIsClosedAndThrowIllegalException();

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
        checkIsClosedAndThrowIllegalException();

        if (memoryConsumption.addAndGet(sizeOf(record)) > config.memoryLimit) {
            synchronized (this) {
                if (memoryConsumption.get() > config.memoryLimit) {
                    checkForPrevTask();
                    memoryStorage = memoryStorage.prepareFlush();

                    int prev = memoryConsumption.getAndSet(sizeOf(record));

                    prepareAndFlush(prev);
                }
            }
        }

        memoryStorage.currentStorage.put(record.getKey(), record);
    }

    private void checkIsClosedAndThrowIllegalException() {
        if (isClosed.get()) {
            throw new IllegalStateException("Dao is closed");
        }
    }

    private void checkForPrevTask() {
        Future<?> future = futureAtomicReference.get();
        if (future != null) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted", e);
            } catch (ExecutionException e) {
                throw new IllegalStateException("Can't to get result", e);
            }
        }
    }

    private boolean needCompact() {
        return memoryStorage.ssTables.size() > config.maxTables;
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

                    SSTable result = SSTable.compact(config.dir, ssTableRanges(memoryStorage, null, null));
                    memoryStorage = memoryStorage.afterCompaction(result);

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
        synchronized (this) {
            isClosed.set(true);

            ThreadUtils.awaitForShutdown(flushExecutor);
            ThreadUtils.awaitForShutdown(executorService);

            if (memoryConsumption.get() > 0) {
                memoryStorage = memoryStorage.prepareFlush();
                SSTable ssTable = flush();
                memoryStorage = memoryStorage.afterFlush(ssTable);
            }
            closeSSTables();
        }
    }

    private void closeSSTables() throws IOException {
        for (SSTable ssTable : memoryStorage.ssTables) {
            ssTable.close();
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

    @GuardedBy("this")
    private void prepareAndFlush(int prevConsumption) {
        futureAtomicReference.set(flushExecutor.submit(() -> {
            try {
                SSTable ssTable = flush();
                memoryStorage = memoryStorage.afterFlush(ssTable);
                futureAtomicReference.set(null);
            } catch (IOException e) {
                memoryConsumption.addAndGet(prevConsumption);
                memoryStorage = memoryStorage.afterFlushException();
                throw new UncheckedIOException(e);
            }
        }));
    }

    private SSTable flush() throws IOException {
        Path dir = config.dir;
        Path file = dir.resolve(SSTable.SAVE_FILE_PREFIX + memoryStorage.ssTables.size());

        return SSTable.save(memoryStorage.storageToWrite.values().iterator(), file);
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

        public Storage afterFlushException() {
            return new Storage(storageToWrite, EMPTY_STORAGE, ssTables);
        }
    }
}

