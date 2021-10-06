package ru.mail.polis.lsm.holod;

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
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LsmDAO implements DAO {
    private static final int MAX_FLUSHES = 4;

    private final ConcurrentLinkedDeque<SSTable> tables = new ConcurrentLinkedDeque<>();
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final DAOConfig config;
    private NavigableMap<ByteBuffer, Record> memoryStorage = newStorage();
    private final NavigableMap<ByteBuffer, Record> memorySnapshot = newStorage();

    private ExecutorService flushExecutor = Executors.newFixedThreadPool(4);
    private final AtomicInteger memoryConsumption = new AtomicInteger(0);
    private final AtomicInteger tableIndex = new AtomicInteger(0);

    private final AtomicInteger currentFlushesCount = new AtomicInteger(0);
    private Future<?> lastFlush;

    public LsmDAO(DAOConfig config) throws IOException {
        this.config = config;
        List<SSTable> ssTables = SSTable.loadFromDir(config.dir);
        tables.addAll(ssTables);
        tableIndex.set(tables.size());
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
        Iterator<Record> memoryRange = new RecordMergingIterator(
                        new PeekingIterator<>(snapshot(fromKey, toKey).values().iterator()),
                        new PeekingIterator<>(map(fromKey, toKey).values().iterator()));
        Iterator<Record> iterator =
                new RecordMergingIterator(
                        new PeekingIterator<>(sstableRanges),
                        new PeekingIterator<>(memoryRange));
        return new TombstoneFilteringIterator(iterator);
    }

    @Override
    public void upsert(Record record) {
        if (memoryConsumption.addAndGet(sizeOf(record)) > config.memoryLimit) {
            synchronized (this) {
                if (memoryConsumption.get() > config.memoryLimit) {
                    // Test passing condition
                    if (currentFlushesCount.get() >= MAX_FLUSHES && lastFlush != null) {
                        try {
                            lastFlush.get();
                        } catch (InterruptedException | ExecutionException ignored) {
                            lastFlush = null;
                        }
                    }
                    asyncFlush();
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
        }
    }

    private NavigableMap<ByteBuffer, Record> newStorage() {
        return new ConcurrentSkipListMap<>();
    }

    private ExecutorService newExecutor() {
        return Executors.newFixedThreadPool(4);
    }

    private int sizeOf(Record record) {
        return SSTable.sizeOf(record);
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            flushExecutor.shutdown();
            try {
                if (!flushExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
                    flushExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            SSTable newTable = flush(tableIndex.getAndIncrement(), memoryStorage);
            tables.add(newTable);
            memorySnapshot.clear();
            memoryStorage = newStorage();
            synchronized (memoryConsumption) {
                memoryConsumption.set(0);
            }
            flushExecutor = newExecutor();
        }
    }

    private void asyncFlush() {
        synchronized (memoryConsumption) {
            final int memoryConsumptionCurrent = memoryConsumption.get();
            final int currentIndex = tableIndex.getAndIncrement();
            final Future<?> currentLastFlush = lastFlush;
            final NavigableMap<ByteBuffer, Record> currentMemoryStorage = memoryStorage;
            memorySnapshot.putAll(currentMemoryStorage);
            memoryStorage = newStorage();
            memoryConsumption.set(0);
            lastFlush = flushExecutor.submit(() -> {
                currentFlushesCount.incrementAndGet();
                try {
                    SSTable newTable = flush(currentIndex, currentMemoryStorage);

                    if (currentLastFlush != null) {
                        currentLastFlush.get();
                    }
                    tables.add(newTable);
                    for (ByteBuffer key: currentMemoryStorage.keySet()) {
                        memorySnapshot.remove(key);
                    }
                } catch (IOException | ExecutionException | InterruptedException e) {
                    synchronized (memoryConsumption) {
                        memoryConsumption.addAndGet(memoryConsumptionCurrent);
                    }
                } finally {
                    currentFlushesCount.decrementAndGet();
                }
            });
        }
    }

    private SSTable flush(int index, NavigableMap<ByteBuffer, Record> memoryStorage) throws IOException {
        Path dir = config.dir;
        Path file = dir.resolve(SSTable.SSTABLE_FILE_PREFIX + index);

        return SSTable.write(memoryStorage.values().iterator(), file);
    }

    private Iterator<Record> sstableRanges(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(tables.size());
        for (SSTable ssTable : tables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return merge(iterators);
    }

    private NavigableMap<ByteBuffer, Record> snapshot(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return navigableMapRange(memorySnapshot, fromKey, toKey);
    }

    private NavigableMap<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return navigableMapRange(memoryStorage, fromKey, toKey);
    }

    private static NavigableMap<ByteBuffer, Record> navigableMapRange(NavigableMap<ByteBuffer, Record> map,
                                                                      @Nullable ByteBuffer fromKey,
                                                                      @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return map;
        } else if (fromKey == null) {
            return map.headMap(toKey, false);
        } else if (toKey == null) {
            return map.tailMap(fromKey, true);
        } else {
            return map.subMap(fromKey, true, toKey, false);
        }
    }

}
