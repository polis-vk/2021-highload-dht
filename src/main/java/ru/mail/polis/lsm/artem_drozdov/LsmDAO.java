package ru.mail.polis.lsm.artem_drozdov;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.alex.FlushTable;

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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class LsmDAO implements DAO {

    private static final Logger LOGGER = LoggerFactory.getLogger(LsmDAO.class);

    private final ConcurrentLinkedDeque<SSTable> tables = new ConcurrentLinkedDeque<>();
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final DAOConfig config;
    private NavigableMap<ByteBuffer, Record> memoryStorage = newStorage();
    private final AtomicInteger memoryConsumption = new AtomicInteger();

    private final ExecutorService flushService;
    private final ArrayList<Future<?>> flushTaskList = new ArrayList<>();

    private final AtomicInteger ssTableIndex = new AtomicInteger();
    private final BlockingQueue<FlushTable> flushQueue;

    public LsmDAO(DAOConfig config) throws IOException {
        this.config = config;
        List<SSTable> ssTables = SSTable.loadFromDir(config.dir);
        tables.addAll(ssTables);
        ssTableIndex.set(tables.size());
        int cores = Runtime.getRuntime().availableProcessors() / 2;
        flushService = Executors.newFixedThreadPool(cores);
        flushQueue = new ArrayBlockingQueue<>(cores + 1);
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
        Iterator<Record> sstableRanges = sstableRanges(fromKey, toKey);
        Iterator<Record> memoryRange = map(fromKey, toKey).values().iterator();
        Iterator<Record> iterator =
                new RecordMergingIterator(
                        new PeekingIterator<>(sstableRanges),
                        new PeekingIterator<>(memoryRange));
        return new TombstoneFilteringIterator(iterator);
    }

    @Override
    public void upsert(Record record) {
        int recordSize = sizeOf(record);
        if (memoryConsumption.addAndGet(recordSize) > config.memoryLimit) {
            synchronized (this) {
                if (memoryConsumption.get() > config.memoryLimit) {
                    try {
                        FlushTable tableToFlush = new FlushTable(ssTableIndex.getAndIncrement(), memoryStorage.values().iterator());
                        flushQueue.put(tableToFlush);
                        memoryStorage = newStorage();
                    } catch (InterruptedException e) {
                        LOGGER.error("Failed put FlushTask to queue!");
                    }

                    int oldConsumption = memoryConsumption.getAndSet(recordSize);
                    flushTaskList.add(flushService.submit(() -> {
                        try {
                            flush(flushQueue.take());
                        } catch (Exception e) {
                            memoryConsumption.addAndGet(oldConsumption);
                            LOGGER.error("Failed flush!");
                        }
                    }));
                }
            }
        }
        memoryStorage.put(record.getKey(), record);
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
            tables.clear();
            tables.add(table);
            memoryStorage = newStorage();
            waitFlushTasks();
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
        synchronized (this) {
            flush(new FlushTable(ssTableIndex.getAndIncrement(), memoryStorage.values().iterator()));
            memoryStorage = newStorage();
            waitFlushTasks();
        }
    }

    private void waitFlushTasks() {
        for (Future<?> task : flushTaskList) {
            try {
                task.get();
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.error("Failed execution flush task!");
            }
        }
    }

    private void flush(FlushTable flushTable) throws IOException {
        Path dir = config.dir;
        Path file = dir.resolve(SSTable.SSTABLE_FILE_PREFIX + flushTable.getIndex());

        SSTable ssTable = SSTable.write(flushTable.getRecords(), file);
        tables.add(ssTable);
    }

    private Iterator<Record> sstableRanges(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(tables.size());
        for (SSTable ssTable : tables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return merge(iterators);
    }

    private NavigableMap<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return memoryStorage;
        } else if (fromKey == null) {
            return memoryStorage.headMap(toKey, false);
        } else if (toKey == null) {
            return memoryStorage.tailMap(fromKey, true);
        } else {
            return memoryStorage.subMap(fromKey, true, toKey, false);
        }
    }

}
