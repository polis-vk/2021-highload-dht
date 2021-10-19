package ru.mail.polis.lsm.artem_drozdov;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.korneev.MemStorage;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LsmDAO implements DAO {

    private static final Logger LOGGER = LoggerFactory.getLogger(LsmDAO.class);

    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final DAOConfig config;
    private final AtomicInteger memoryConsumption = new AtomicInteger();
    private volatile MemStorage memoryStorage;

    private final ExecutorService compactService = Executors.newSingleThreadExecutor();

    public LsmDAO(DAOConfig config) throws IOException {
        this.config = config;
        this.memoryStorage = MemStorage.init(SSTable.loadFromDir(config.dir));
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

        MemStorage memStorage = this.memoryStorage;

        Iterator<Record> sstableRanges = sstableRanges(memStorage, fromKey, toKey);

        Iterator<Record> currentMemoryRange = map(memStorage.currentStorage, fromKey, toKey).values().iterator();
        Iterator<Record> toWriteMemoryRange = map(memStorage.storageToWrite, fromKey, toKey).values().iterator();
        Iterator<Record> memory = new RecordMergingIterator(
                new PeekingIterator<>(currentMemoryRange),
                new PeekingIterator<>(toWriteMemoryRange)
        );

        Iterator<Record> iterator =
                new RecordMergingIterator(
                        new PeekingIterator<>(sstableRanges),
                        new PeekingIterator<>(memory));

        return new TombstoneFilteringIterator(iterator);
    }

    @Override
    public void upsert(Record record) {
        int recordSize = sizeOf(record);
        if (memoryConsumption.addAndGet(recordSize) > config.memoryLimit) {
            synchronized (this) {
                if (memoryConsumption.get() > config.memoryLimit) {
                    memoryStorage = memoryStorage.prepareFlush();

                    int prev = memoryConsumption.getAndSet(recordSize);

                    try {
                        SSTable ssTable = flush();
                        memoryStorage = memoryStorage.afterFlush(ssTable);
                        if (needCompact()) {
                            compact();
                        }
                    } catch (IOException e) {
                        memoryConsumption.addAndGet(prev);
                        throw new UncheckedIOException(e);
                    }
                }
            }
        }

        memoryStorage.currentStorage.put(record.getKey(), record);
    }

    private boolean needCompact() {
        return memoryStorage.tableList.size() > config.maxTables;
    }

    @Override
    public void compact() {
        compactService.execute(() -> {
            synchronized (this) {
                try {
                    if (!needCompact()) {
                        return;
                    }


                    SSTable result;
                    try {
                        result = SSTable.compact(config.dir, sstableRanges(memoryStorage, null, null));
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }

                    memoryStorage = memoryStorage.afterCompaction(result);
                } catch (Exception e) {
                    LOGGER.error("Can npt run compaction", e);
                }
            }
        });
    }

    private int sizeOf(Record record) {
        return SSTable.sizeOf(record);
    }

    @Override
    public void close() throws IOException {
        compactService.shutdown();
        try {
            if (!compactService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                throw new IllegalStateException("Can not await for termination");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }

        synchronized (this) {
            memoryStorage = memoryStorage.prepareFlush();
            flush();
            memoryStorage = null;
        }
    }

    @GuardedBy("this")
    private SSTable flush() throws IOException {
        Path dir = config.dir;
        Path file = dir.resolve(SSTable.SSTABLE_FILE_PREFIX + memoryStorage.tableList.size());

        return SSTable.write(memoryStorage.storageToWrite.values().iterator(), file);
    }

    private Iterator<Record> sstableRanges(MemStorage memStorage, @Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(memStorage.tableList.size());
        for (SSTable ssTable : memStorage.tableList) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return merge(iterators);
    }

    private NavigableMap<ByteBuffer, Record> map(NavigableMap<ByteBuffer, Record> storage, @Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
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
}
