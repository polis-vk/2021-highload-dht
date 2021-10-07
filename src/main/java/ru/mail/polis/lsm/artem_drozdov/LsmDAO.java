package ru.mail.polis.lsm.artem_drozdov;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.artem_drozdov.util.RecordIterators;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class LsmDAO implements DAO {

    private final long flushMemoryThreshold;
    private ExecutorService executorService = Executors.newWorkStealingPool();
    private final AtomicInteger runningFlushesCount = new AtomicInteger(0);

    private final AtomicInteger currentIndex = new AtomicInteger(0);
    private final ConcurrentLinkedDeque<NavigableMap<ByteBuffer, Record>> flushingStorages =
            new ConcurrentLinkedDeque<>();
    private NavigableMap<ByteBuffer, Record> memoryStorage;
    private final ConcurrentLinkedDeque<SSTable> tables = new ConcurrentLinkedDeque<>();
    private final ArrayDeque<Future<?>> runningFlushes = new ArrayDeque<>();

    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final DAOConfig config;

    private final AtomicInteger memoryConsumption = new AtomicInteger();

    public LsmDAO(DAOConfig config) throws IOException {
        this.config = config;
        this.flushMemoryThreshold = config.memoryLimit * 2L;
        List<SSTable> ssTables = SSTable.loadFromDir(config.dir);
        tables.addAll(ssTables);
        currentIndex.set(tables.size());
        memoryStorage = newStorage();
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        Iterator<Record> sstableRanges = sstableRanges(fromKey, toKey);
        Iterator<Record> memoryRange;
        synchronized (this) {
            memoryRange = new RecordIterators.MergingIterator(
                    new RecordIterators.PeekingIterator(
                            RecordIterators.merge(flushingStorages.stream().map(storage -> map(storage, fromKey, toKey))
                                    .collect(Collectors.toList()))
                    ),
                    new RecordIterators.PeekingIterator(map(memoryStorage, fromKey, toKey)));
        }
        Iterator<Record> iterator = new RecordIterators.MergingIterator(
                new RecordIterators.PeekingIterator(sstableRanges),
                new RecordIterators.PeekingIterator(memoryRange)
        );
        return RecordIterators.filterTombstones(iterator);
    }

    @Override
    public void upsert(Record record) {
        if (memoryConsumption.addAndGet(sizeOf(record)) > config.memoryLimit) {
            synchronized (this) {
                if (memoryConsumption.get() > config.memoryLimit) {
                    runningFlushes.removeIf(Future::isDone);
                    while (!runningFlushes.isEmpty() && runningFlushes.peek().isDone()) {
                        runningFlushes.poll();
                    }

                    while (Runtime.getRuntime().freeMemory() < this.flushMemoryThreshold && !runningFlushes.isEmpty()) {
                        try {
                            runningFlushes.poll().get();
                        } catch (ExecutionException e) {
                            break;
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }

                    synchronized (memoryConsumption) {
                        final int flushIndex = currentIndex.getAndAdd(1);
                        final Future<?> future = executorService.submit(
                                this.runnableFlush(flushIndex, memoryStorage, memoryConsumption.get(),
                                        getLastSubmittedFlush())
                        );
                        runningFlushes.add(future);

                        flushingStorages.add(memoryStorage);
                        memoryStorage = newStorage();
                        memoryConsumption.set(sizeOf(record));
                    }
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
            currentIndex.set(1);
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
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(30, TimeUnit.MINUTES)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            executorService = Executors.newWorkStealingPool();
            synchronized (memoryConsumption) {
                final int flushIndex = currentIndex.getAndIncrement();
                try {
                    flush(flushIndex, memoryStorage, memoryConsumption.get(), getLastSubmittedFlush());
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof IOException) {
                        throw new UncheckedIOException((IOException) e.getCause());
                    }
                }
                memoryConsumption.set(0);
                memoryStorage = newStorage();
            }
        }
    }

    private synchronized @Nullable Future<?> getLastSubmittedFlush() {
        return runningFlushes.isEmpty() ? null : runningFlushes.peek();
    }

    private Callable<Void> runnableFlush(int index, NavigableMap<ByteBuffer, Record> memoryStorage,
                                   int memoryTableConsumption, @Nullable Future<?> prevFlush) {
        return () -> {
            this.runningFlushesCount.incrementAndGet();
            flush(index, memoryStorage, memoryTableConsumption, prevFlush);
            this.runningFlushesCount.decrementAndGet();
            return null;
        };
    }

    private void flush(int index,
                       NavigableMap<ByteBuffer, Record> memoryStorage,
                       int memoryTableConsumption,
                       @Nullable Future<?> prevFlush) throws ExecutionException {
        try {
            Path dir = config.dir;
            Path file = dir.resolve(SSTable.SSTABLE_FILE_PREFIX + index);
            SSTable ssTable = SSTable.write(memoryStorage.values().iterator(), file);

            if (prevFlush != null) {
                prevFlush.get();
            }
            tables.add(ssTable);
            flushingStorages.poll();
        } catch (ExecutionException | IOException | InterruptedException e) {
            synchronized (memoryConsumption) {
                this.memoryStorage.putAll(memoryStorage);
                memoryConsumption.getAndAdd(memoryTableConsumption);
            }
            throw new ExecutionException(e);
        }
    }

    private Iterator<Record> sstableRanges(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(tables.size());
        for (SSTable ssTable : tables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return RecordIterators.merge(iterators);
    }

    private Iterator<Record> map(SortedMap<ByteBuffer, Record> map,
                                 @Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return map.values().iterator();
        }
        if (fromKey == null) {
            return map.headMap(toKey).values().iterator();
        }
        if (toKey == null) {
            return map.tailMap(fromKey).values().iterator();
        }
        return map.subMap(fromKey, toKey).values().iterator();
    }
}
