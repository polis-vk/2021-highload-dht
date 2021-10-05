package ru.mail.polis.lsm.artemdrozdov;

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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class LsmDAO implements DAO {

    private NavigableMap<ByteBuffer, Record> memoryStorage = newStorage();
    private final ConcurrentLinkedDeque<SSTable> tables = new ConcurrentLinkedDeque<>();
    private volatile boolean flushDone = false;//for not to flush twice
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final DAOConfig config;
    static private final int POLL_LIMIT = 20 * 1024 * 1024;
    private final AtomicInteger pollPayload = new AtomicInteger(0);

    @GuardedBy("this")
    private final ExecutorService writeService = Executors.newFixedThreadPool(4);
    private final AtomicInteger memoryConsumption = new AtomicInteger();
    private final AtomicInteger sstablesCtr = new AtomicInteger(0);

    public LsmDAO(DAOConfig config) throws IOException {
        this.config = config;
        List<SSTable> ssTables = SSTableDirHelper.loadFromDir(config.dir);
        tables.addAll(ssTables);
        sstablesCtr.set(tables.size());
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        Iterator<Record> sstableRanges = sstableRanges(fromKey, toKey);
        Iterator<Record> memoryRange = map(fromKey, toKey).values().iterator();
        Iterator<Record> iterator = mergeTwo(new PeekingIterator(sstableRanges), new PeekingIterator(memoryRange));
        return filterTombstones(iterator);
    }

    @Override
    public void upsert(Record record) {
        if (memoryConsumption.addAndGet(sizeOf(record)) > config.memoryLimit) {
            int currMemoryConsumption = memoryConsumption.get();//save for future IOException processing
            memoryConsumption.set(sizeOf(record));//set to close if
            flushDone = false;
            synchronized (this) {//TODO replace me with semaphore please
                if (!flushDone) {//multiple flushing within race handling

                    //make snapshot for flushing to avoid parallel writing conflicts
                    NavigableMap<ByteBuffer, Record> memorySnapshot = memoryStorage;
                    memoryStorage = newStorage();

                    Runnable flushLambda = () -> {
                        {
                            try {
                                flush(memorySnapshot, sstablesCtr.getAndIncrement());
                                flushDone = true;
                            } catch (IOException e) {//exception processing instead of deferred future analyzing
                                System.out.println("IOException caught");
                                e.printStackTrace();
                                memoryConsumption.set(currMemoryConsumption);
                                memoryStorage.putAll(memorySnapshot);//parallelAddedWhileWeWasFlushingRecords merge wasNotFlushedRecords
                            } finally {
                                pollPayload.addAndGet(-currMemoryConsumption);
                            }
                        }
                    };

//                    System.out.println("runTimeFree = " + Runtime.getRuntime().freeMemory() / 1024 / 1024);
//                    System.out.println("pollPayload = " + pollPayload.get() / 1024 / 1024);
                    if (pollPayload.addAndGet(currMemoryConsumption) < POLL_LIMIT) {//run async if have memory
                        System.out.println("run flush in poll");
                        writeService.submit(flushLambda);
                    } else {//run sequential if have no memory
                        System.out.println("run flush sequentially");
                        flushLambda.run();
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
            flush(memoryStorage, sstablesCtr.getAndIncrement());
        }
    }

    private void flush(NavigableMap<ByteBuffer, Record> storageSnapshot, int fileId) throws IOException {
        Path dir = config.dir;
        Path file = dir.resolve(SSTable.SSTABLE_FILE_PREFIX + fileId);

        SSTable ssTable = SSTable.write(storageSnapshot.values().iterator(), file);
        tables.add(ssTable);
    }

    private Iterator<Record> sstableRanges(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(tables.size());
        for (SSTable ssTable : tables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return merge(iterators);
    }

    private SortedMap<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
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
