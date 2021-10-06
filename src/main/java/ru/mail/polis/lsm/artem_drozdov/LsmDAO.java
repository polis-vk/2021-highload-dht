package ru.mail.polis.lsm.artem_drozdov;

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
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class LsmDAO implements DAO {

    private static final Logger LOG = LoggerFactory.getLogger(LsmDAO.class);
    private final ExecutorService FLUSH_EXECUTOR = Executors.newSingleThreadScheduledExecutor();

    private volatile MemTable memTable;
    private final ConcurrentLinkedDeque<SSTable> tables = new ConcurrentLinkedDeque<>();
    public final ConcurrentLinkedQueue<MemTable> flushedMemTables = new ConcurrentLinkedQueue<>();

    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final DAOConfig config;

    private final AtomicInteger memoryConsumption = new AtomicInteger();
    private final AtomicInteger memTableActiveUsers = new AtomicInteger();

    /**
     * Create LsmDAO from config.
     *
     * @param config - LamDAo config
     * @throws IOException - in case of io exception
     */
    public LsmDAO(DAOConfig config) throws IOException {
        this.config = config;
        List<SSTable> ssTables = SSTable.loadFromDir(config.dir);
        tables.addAll(ssTables);
        memTable = MemTable.newStorage(tables.size());
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        synchronized (this) {
            Iterator<Record> sstableRanges = sstableRanges(fromKey, toKey);
            Iterator<Record> memoryRange = map(memTable, fromKey, toKey).values().iterator();
            Iterator<Record> iterator = mergeTwo(new PeekingIterator(sstableRanges), new PeekingIterator(memoryRange));

            List<Iterator<Record>> flushedTables = new ArrayList<>(flushedMemTables.size());
            for (MemTable flushedMemTable : flushedMemTables) {
                Iterator<Record> flushMemoryRange = map(flushedMemTable, fromKey, toKey).values().iterator();
                flushedTables.add(flushMemoryRange);
            }

            iterator = mergeTwo(new PeekingIterator(iterator), new PeekingIterator(merge(flushedTables)));
            return filterTombstones(iterator);
        }
    }

    @Override
    public void upsert(Record record) {
        memTableActiveUsers.incrementAndGet();
        while (memoryConsumption.addAndGet(sizeOf(record)) > config.memoryLimit) {
            memTableActiveUsers.decrementAndGet();

            // Это остатки костыля для исправления ошибки OutOfMemoryException,
            // без которых все в один момент заработало. Но если вдруг...
//            while (flushedMemTables.size() > 3) {
//                Thread.onSpinWait();
//            }

            synchronized (this) {
                // Если в данный блок попали друг за другом два и более потока, значит каждый из них
                // увеличил общий счетчик ранее, а первый пришедший сбросил его.
                // Для остальных потоков нужно снова его увеличить. С этим поможет while.
                if (memoryConsumption.get() > config.memoryLimit) {
                    // Применяем активное ожидание, чтобы избавиться от
                    // медленного и вредного потока, который никак не может
                    // записать своё значение в memTable после всех проверок.
                    while (memTableActiveUsers.get() > 0) {
                        Thread.onSpinWait();
                    }

                    scheduleFlush(memTable);

                    memoryConsumption.getAndSet(sizeOf(record));
                    break;
                }
            }
            memTableActiveUsers.incrementAndGet();
        }

        memTable.put(record.getKey(), record);
        memTableActiveUsers.decrementAndGet();
    }

    @Override
    public void closeAndCompact() {
        synchronized (this) {
            SSTable table;
            try {
                table = SSTable.compact(config.dir, range(null, null));
            } catch (IOException e) {
                throw new UncheckedIOException("Can't compact", e);
            }
            tables.clear();
            tables.add(table);
            memTable = MemTable.newStorage(tables.size());
        }
    }

    private int sizeOf(Record record) {
        return SSTable.sizeOf(record);
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            flush(memTable);
        }
    }

    @GuardedBy("this")
    private void scheduleFlush(MemTable memTable) {
        flushedMemTables.add(memTable);
        scheduleFlush();
        this.memTable = MemTable.newStorage(memTable.getId() + 1);
    }

    private void scheduleFlush() {
        FLUSH_EXECUTOR.submit(() -> {
            synchronized (LsmDAO.this) {
                MemTable memTable = flushedMemTables.poll();
                if (memTable == null) return;

                try {
                    flush(memTable);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        });
    }

    @GuardedBy("this")
    private void flush(MemTable memTable) throws IOException {
        Path dir = config.dir;
        Path file = dir.resolve(SSTable.SSTABLE_FILE_PREFIX + memTable.getId());

        SSTable ssTable = SSTable.write(memTable.values().iterator(), file);
        tables.add(ssTable);
    }

    @GuardedBy("this")
    private Iterator<Record> sstableRanges(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(tables.size());
        for (SSTable ssTable : tables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return merge(iterators);
    }

    @GuardedBy("this")
    private SortedMap<ByteBuffer, Record> map(MemTable memTable, @Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return memTable;
        }
        if (fromKey == null) {
            return memTable.headMap(toKey);
        }
        if (toKey == null) {
            return memTable.tailMap(fromKey);
        }
        return memTable.subMap(fromKey, toKey);
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
        return new MergeIterator(left, right);
    }

    private static Iterator<Record> filterTombstones(Iterator<Record> iterator) {
        PeekingIterator delegate = new PeekingIterator(iterator);
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                for (;;) {
                    Record peek = delegate.peek();
                    if (peek == null) {
                        return false;
                    }
                    if (!peek.isTombstone()) {
                        return true;
                    }

                    delegate.next();
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
