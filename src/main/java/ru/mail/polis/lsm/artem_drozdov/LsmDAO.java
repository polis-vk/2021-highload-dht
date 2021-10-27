package ru.mail.polis.lsm.artem_drozdov;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class LsmDAO implements DAO {

    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final DAOConfig config;
    private static final Logger logger = LoggerFactory.getLogger(LsmDAO.class);
    private final AtomicInteger memoryConsumption = new AtomicInteger();
    private final ExecutorService flushExecutor = Executors.newSingleThreadExecutor();
    private final ExecutorService compactExecutor = Executors.newSingleThreadExecutor();
    private CompletableFuture<Void> flushCompletable;
    private volatile Storage storage;

    public LsmDAO(DAOConfig config) throws IOException {
        this.config = config;
        this.storage = Storage.init(SSTableDirHelper.loadFromDir(config.dir));
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        Iterator<Record> sstableRanges = sstableRanges(storage,fromKey, toKey);
        Iterator<Record> memoryRange = map(storage.currentStorage, fromKey, toKey).values().iterator();
        Iterator<Record> tmpMemoryRange = map(storage.tmpStorage,fromKey,toKey).values().iterator();
        Iterator<Record> memory = mergeTwo(new PeekingIterator(memoryRange), new PeekingIterator(tmpMemoryRange));
        Iterator<Record> iterator = mergeTwo(new PeekingIterator(sstableRanges), new PeekingIterator(memory));
        return filterTombstones(iterator);
    }

    @Override
    public void upsert(Record record) {
        int actualMemoryConsumption = memoryConsumption.addAndGet(sizeOf(record));
        if(actualMemoryConsumption > config.memoryLimit){
            //logger.info("Going to flush...{}",actualMemoryConsumption);
            synchronized (this){
                if(memoryConsumption.get() > config.memoryLimit){
                    int oldMemoryConsumption = memoryConsumption.getAndSet(sizeOf(record));
                    flushCompletable = CompletableFuture.runAsync(() ->{
                        try{
                            //logger.info("Flush started..");
                            storage = storage.prepareFlush();
                            SSTable ssTable = flush();
                            storage = storage.afterFlush(ssTable);
                            if (needCompact()){
                                performCompact();
                            }
                            //logger.info("Flush finished...");
                        } catch (IOException e) {
                            logger.error("Flush failed...", e);
                            memoryConsumption.addAndGet(oldMemoryConsumption);
                            throw new UncheckedIOException(e);
                        }
                    }, flushExecutor);
                    awaitTaskComplete();
                } else{
                    logger.info("Concurrent flush...");
                }
            }
        }
        storage.currentStorage.put(record.getKey(),record);
    }

    private boolean needCompact(){
        return storage.tables.size() > config.maxTables;
    }

    @Override
    public void compact() {
        compactExecutor.execute(() -> {
            synchronized (this) {
                performCompact();
            }
        });
    }

    private void performCompact(){
        try {
            if (!needCompact()) {
                return;
            }
            //logger.info("Compact started...");
            SSTable compactTable;
            try {
                compactTable = SSTable.compact(config.dir, range(null, null));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            storage = storage.afterCompaction(compactTable);
            //logger.info("Compact finished...");
        } catch (Exception e) {
            logger.error("Can't compact...", e);
        }
    }
    private void awaitTaskComplete(){
        if (flushCompletable == null){
            return;
        }
        try{
            flushCompletable.get();
        } catch (InterruptedException | ExecutionException e ){
            Thread.currentThread().interrupt();
            logger.error("Can't wait future complete execution....",e);
        }
    }

    private int sizeOf(Record record) {
        return SSTable.sizeOf(record);
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            storage = storage.prepareFlush();
            flush();
        }
    }

    private SSTable flush() throws IOException {
        Path dir = config.dir;
        Path file = dir.resolve(SSTable.SSTABLE_FILE_PREFIX + storage.tables.size());
        return SSTable.write(storage.tmpStorage.values().iterator(), file);
    }

    private Iterator<Record> sstableRanges(Storage storage,@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(storage.tables.size());
        for (SSTable ssTable : storage.tables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return merge(iterators);
    }

    private SortedMap<ByteBuffer, Record> map(NavigableMap<ByteBuffer,Record>storage,@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return storage;
        }
        if (fromKey == null) {
            return storage.headMap(toKey);
        }
        if (toKey == null) {
            return storage.tailMap(fromKey);
        }
        return storage.subMap(fromKey, toKey);
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