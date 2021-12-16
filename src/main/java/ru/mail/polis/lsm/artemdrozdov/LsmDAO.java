package ru.mail.polis.lsm.artemdrozdov;

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
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LsmDAO implements DAO {

    private static final Logger LSM_LOGGER = LoggerFactory.getLogger(LsmDAO.class);

    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final DAOConfig config;
    @SuppressWarnings("PMD.AvoidUsingVolatile")
    private volatile SmartStorage smartStorage = SmartStorage.createEmpty();

    private static final int POLL_LIMIT = 20 * 1024 * 1024;
    private final AtomicInteger pollPayload = new AtomicInteger(0);
    private final ExecutorService writeService = Executors.newSingleThreadExecutor();

    private final AtomicInteger memoryConsumption = new AtomicInteger();
    private final AtomicInteger sstablesCtr = new AtomicInteger(0);

    public LsmDAO(DAOConfig config) throws IOException {
        this.config = config;
        List<SSTable> ssTables = SSTableDirHelper.loadFromDir(config.dir);
        smartStorage.tables.addAll(ssTables);
        sstablesCtr.set(smartStorage.tables.size());
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        SmartStorage savedStorage = smartStorage;
        Iterator<Record> sstableRanges = sstableRanges(savedStorage.tables, fromKey, toKey);
        Iterator<Record> snapshotRange = map(savedStorage.memorySnapshot, fromKey, toKey).values().iterator();
        Iterator<Record> memoryRange = map(savedStorage.memory, fromKey, toKey).values().iterator();
        Iterator<Record> iterator = mergeTwo(
                new PeekingIterator(mergeTwo(new PeekingIterator(sstableRanges), new PeekingIterator(snapshotRange))),
                new PeekingIterator((memoryRange))
        );
        return filterTombstones(iterator);
    }

    @GuardedBy("this")
    @Override
    public void upsert(Record record) {
        int currMemoryConsumption = memoryConsumption.addAndGet(sizeOf(record));//save for future IOException processing
        if (currMemoryConsumption > config.memoryLimit) {
            synchronized (this) {
                if (memoryConsumption.get() > config.memoryLimit) {
                    memoryConsumption.set(sizeOf(record));//set to close if

                    SmartStorage smartStorageLink = SmartStorage.fromSnapshot(smartStorage.memory, smartStorage.tables);
                    smartStorage = smartStorageLink;

                    Runnable flushLambda = () -> {
                        {
                            try {
                                flush(smartStorageLink.memorySnapshot, sstablesCtr.getAndIncrement());
                            } catch (IOException e) {
                                //exception processing instead of deferred future analyzing
                                LSM_LOGGER.error("Error in flush caught", e);
                                memoryConsumption.addAndGet(currMemoryConsumption);
                                smartStorage.memory.putAll(smartStorageLink.memorySnapshot);
                            } finally {
                                pollPayload.addAndGet(-currMemoryConsumption);
                            }
                        }
                    };

                    if (pollPayload.addAndGet(currMemoryConsumption) < POLL_LIMIT) {
                        //run async if have memory
                        LSM_LOGGER.debug("Async flushing");
                        writeService.submit(flushLambda);
                    } else {
                        //run sequential if have no memory
                        LSM_LOGGER.debug("Sync flushing");
                        flushLambda.run();
                    }

                }
            }
        }

        smartStorage.memory.put(record.getKey(), record);
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
            smartStorage.tables.clear();
            smartStorage.tables.add(table);
            smartStorage = SmartStorage.createFromTables(smartStorage.tables);
        }
    }

    private int sizeOf(Record record) {
        return SSTable.sizeOf(record);
    }

    @GuardedBy("this")
    @Override
    public void close() throws IOException {
        writeService.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!writeService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS)) {
                writeService.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!writeService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS)) {
                    LSM_LOGGER.error("Pool did not terminate");
                }
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            writeService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        synchronized (this) {
            flush(smartStorage.memory, sstablesCtr.getAndIncrement());
        }
    }

    private void flush(NavigableMap<ByteBuffer, Record> storageSnapshot, int fileId) throws IOException {
        Path dir = config.dir;
        Path file = dir.resolve(SSTable.SSTABLE_FILE_PREFIX + fileId);

        SSTable ssTable = SSTable.write(storageSnapshot.values().iterator(), file);
        smartStorage.tables.add(ssTable);
    }

    private Iterator<Record> sstableRanges(
            ConcurrentLinkedDeque<SSTable> tables,
            @Nullable ByteBuffer fromKey,
            @Nullable ByteBuffer toKey
    ) {
        List<Iterator<Record>> iterators = new ArrayList<>(tables.size());
        for (SSTable ssTable : tables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return merge(iterators);
    }

    private SortedMap<ByteBuffer, Record> map(
            NavigableMap<ByteBuffer, Record> memoryStorage,
            @Nullable ByteBuffer fromKey,
            @Nullable ByteBuffer toKey) {
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

                    if (delegate.hasNext()) {
                        delegate.next();
                    }
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
