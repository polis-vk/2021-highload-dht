package ru.mail.polis.lsm.alexnifontov;

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
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LsmDAO implements DAO {

    private static final int MEMORY_LIMIT = 32 * 1024 * 1024;
    private final ConcurrentLinkedDeque<SSTable> tables = new ConcurrentLinkedDeque<>();
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final DAOConfig config;
    private NavigableMap<ByteBuffer, Record> memoryStorage = newStorage();
    private final NavigableMap<ByteBuffer, Record> flushingMemory = newStorage();

    private final AtomicInteger globalMemoryConsumption = new AtomicInteger();
    private final AtomicInteger localMemoryConsumption = new AtomicInteger();
    private final AtomicInteger currentIndex = new AtomicInteger();

    private ExecutorService executor = Executors.newWorkStealingPool();
    private final NavigableMap<Integer, Future<?>> flushFutures = new ConcurrentSkipListMap<>();

    public LsmDAO(DAOConfig config) throws IOException {
        this.config = config;
        List<SSTable> ssTables = SSTable.loadFromDir(config.dir);
        tables.addAll(ssTables);
        currentIndex.set(tables.size());
    }

    public class Flush implements Runnable {
        final int index;
        final NavigableMap<ByteBuffer, Record> map;
        final int fallbackMemory;

        public Flush(int index,
                     NavigableMap<ByteBuffer, Record> map,
                     int fallbackMemory) {
            this.map = map;
            this.index = index;
            this.fallbackMemory = fallbackMemory;
        }

        @Override
        public void run() {
            try {
                Path file = config.dir.resolve(SSTable.SSTABLE_FILE_PREFIX + index);
                SSTable ssTable = SSTable.write(map.values().iterator(), file);

                for (Future<?> sync : flushFutures.headMap(index).values()) {
                    sync.get();
                }
                flushFutures.remove(index);
                tables.add(ssTable);

                for (ByteBuffer index : map.keySet()) {
                    flushingMemory.remove(index);
                }
            } catch (IOException | InterruptedException | ExecutionException e) {
                synchronized (localMemoryConsumption) {
                    memoryStorage.putAll(map);
                    localMemoryConsumption.addAndGet(fallbackMemory);
                }
            } finally {
                globalMemoryConsumption.addAndGet(-fallbackMemory);
            }
        }
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
                new PeekingIterator<>(map(flushingMemory, fromKey, toKey).values().iterator()),
                new PeekingIterator<>(map(memoryStorage, fromKey, toKey).values().iterator())
        );
        Iterator<Record> iterator =
                new RecordMergingIterator(
                        new PeekingIterator<>(sstableRanges),
                        new PeekingIterator<>(memoryRange));
        return new TombstoneFilteringIterator(iterator);
    }

    @Override
    public void upsert(Record record) {
        if (localMemoryConsumption.addAndGet(sizeOf(record)) > config.memoryLimit) {
            synchronized (this) {
                if (localMemoryConsumption.get() > config.memoryLimit) {
                    flush();
                }
            }
        }
        memoryStorage.put(record.getKey(), record);
    }

    @Override
    public void closeAndCompact() {
        synchronized (this) {
            try {
                if (flushFutures.lastEntry() != null) {
                    flushFutures.lastEntry().getValue().get();
                }
            } catch (InterruptedException | ExecutionException e) {
                executor.shutdownNow();
                executor = Executors.newWorkStealingPool();
            }

            final SSTable table;
            try {
                table = SSTable.compact(config.dir, range(null, null));
            } catch (IOException e) {
                throw new UncheckedIOException("Can't compact", e);
            }
            tables.clear();
            tables.add(table);
            currentIndex.set(1);
            synchronized (localMemoryConsumption) {
                localMemoryConsumption.set(0);
                memoryStorage = newStorage();
            }
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
            flush();
            executor.shutdown();
            try {
                if (executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
            executor = Executors.newWorkStealingPool();
        }
    }

    private void flush() {
        Iterator<Future<?>> flushFuturesIt = flushFutures.values().iterator();
        while (globalMemoryConsumption.get() >= MEMORY_LIMIT && flushFuturesIt.hasNext()) {
            try {
                flushFuturesIt.next().get();
            } catch (InterruptedException | ExecutionException e) {
                break;
            }
        }

        globalMemoryConsumption.addAndGet(localMemoryConsumption.get());
        final Future<?> flushFuture = executor.submit(
                new Flush(
                        currentIndex.getAndIncrement(),
                        memoryStorage,
                        localMemoryConsumption.get()
                ));
        flushFutures.put(currentIndex.get(), flushFuture);
        flushingMemory.putAll(memoryStorage);

        synchronized (localMemoryConsumption) {
            localMemoryConsumption.set(0);
            memoryStorage = newStorage();
        }
    }

    private Iterator<Record> sstableRanges(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(tables.size());
        for (SSTable ssTable : tables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return merge(iterators);
    }

    private static NavigableMap<ByteBuffer, Record> map(NavigableMap<ByteBuffer, Record> memoryStorage,
                                                        @Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
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
