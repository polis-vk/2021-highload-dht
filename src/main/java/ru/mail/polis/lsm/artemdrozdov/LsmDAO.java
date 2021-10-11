package ru.mail.polis.lsm.artemdrozdov;

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
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class LsmDAO implements DAO {

    private NavigableMap<ByteBuffer, Record> memoryStorage = newStorage();
    private TableStorage tableStorage;
    // блокирующая очередь используется, чтобы флашить данные в порядке поступления
    private final BlockingQueue<NavigableMap<ByteBuffer, Record>> circBuffer;
    // rangeBuffer нужен, чтобы обеспечить доступ к данным во время flush,
    // так как возможно, что зайдут в upsert сразу несколько потоков с большой записью и начнут флашить,
    // используется коллекция для поддержания доступа к данным
    private final List<NavigableMap<ByteBuffer, Record>> rangeBuffer;

    private final ExecutorService flushExecutor = Executors.newSingleThreadExecutor();
    private final ExecutorService compactExecutor = Executors.newSingleThreadExecutor();

    private final DAOConfig config;

    private final AtomicLong memoryConsumption;
    // общее количество таблица
    private final AtomicLong tableCounter;
    // таблицы которые успели зафлашиться, перед компактом
    private final AtomicInteger sizeBeforeCompact;
    // индексатор для rangeBuffer - использует во flush,
    // чтобы поддежривать упорядоченный доступ во время range
    private final AtomicInteger idxRangeBuffer;
    // размер очереди
    private final int queueSize;

    /**
     *  Create LsmDAO from config.
     *
     * @param config - LamDAo config
     * @param queueSize - каунтер для блок. очереди; слишком большое значение ведет к OutOfMemory Exception
     * @throws IOException - in case of io exception
     */
    public LsmDAO(DAOConfig config, final int queueSize) throws IOException {
        this.memoryConsumption = new AtomicLong();
        this.circBuffer = new ArrayBlockingQueue<>(queueSize);
        this.rangeBuffer = new CopyOnWriteArrayList<>();
        for (int i = 0; i < queueSize; ++i) {
            rangeBuffer.add(newStorage());
        }
        this.idxRangeBuffer = new AtomicInteger(0);
        this.queueSize = queueSize;
        this.config = config;
        List<SSTable> ssTables = SSTable.loadFromDir(config.dir);
        this.tableStorage = new TableStorage(ssTables);
        tableCounter = new AtomicLong(this.tableStorage.tables.size());
        this.sizeBeforeCompact = new AtomicInteger(0);
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        Iterator<Record> sstableRanges = sstableRanges(fromKey, toKey);
        Iterator<Record> memoryRange = map(fromKey, toKey, memoryStorage).values().iterator();
        Iterator<NavigableMap<ByteBuffer, Record>> queueRange = circBuffer.iterator();

        // Эта часть тербует оптимизации, хотя в целом, реализация работает быстрее.
        // Оптимизировать после основной части...
        while (queueRange.hasNext()) {
            Iterator<Record> unionRange = map(fromKey, toKey, queueRange.next()).values().iterator();
            memoryRange = mergeTwo(unionRange, memoryRange);
        }
        Iterator<Record> iterator = mergeTwo(sstableRanges, memoryRange);
        return filterTombstones(iterator);
    }

    public boolean greaterThanCAS(final int maxValue, final int newSize) {
        return (memoryConsumption.getAndUpdate(size -> (size + newSize) > maxValue ? newSize : size) + newSize) > maxValue;
    }

    @Override
    public void upsert(Record record) {

        if (greaterThanCAS(config.memoryLimit, sizeOf(record))) {

            try {
                circBuffer.put(memoryStorage);
                memoryStorage = newStorage();
            } catch (InterruptedException e) {
                putRecord(record);
                Thread.currentThread().interrupt();
                return;
            }

            flushExecutor.execute(() -> {
                final int rollbackSize = sizeOf(record);
                final int localIdx = idxRangeBuffer.getAndUpdate(i -> (i + 1) % queueSize);
                try {
                    rangeBuffer.set(localIdx, circBuffer.take());
                    SSTable flushTable = flush(rangeBuffer.get(localIdx));
                    this.tableStorage = tableStorage.afterFlush(flushTable);
                } catch (IOException | InterruptedException e) {
                    memoryConsumption.addAndGet(-rollbackSize);
                    memoryStorage.putAll(rangeBuffer.get(localIdx)); // restore data + new data
                    Thread.currentThread().interrupt();
                } finally {
                    rangeBuffer.set(localIdx, newStorage());
                }
            });

            compactExecutor.execute(() -> {
                synchronized (this) {
                    if (tableStorage.isCompact()) {
                        compact();
                    }
                }
            });

        } else {
            memoryConsumption.addAndGet(sizeOf(record));
        }

        putRecord(record);
    }

    @Override
    public void compact() {
        try {
            sizeBeforeCompact.set(tableStorage.tables.size());
            SSTable table = perfomCompact();
            this.tableStorage = tableStorage.afterCompact(table);
        } catch (IOException e) {
            throw new UncheckedIOException("Can't compact", e);
        }
    }

    private SSTable perfomCompact() throws IOException {
        return SSTable.compact(config.dir, range(null, null));
    }

    private void putRecord(Record record) {
        memoryStorage.put(record.getKey(), record);
    }

    private NavigableMap<ByteBuffer, Record> newStorage() {
        return new ConcurrentSkipListMap<>();
    }

    private int sizeOf(Record record) {
        return SSTable.sizeOf(record);
    }

    @Override
    public void close() throws IOException {
        // после изменения CompletebleFeature.runAsync на ExecutorService,
        // время выполнения тестов hugeRecord увеличилось
        flushExecutor.shutdown();
        compactExecutor.shutdown();
        try {
            if (!flushExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("Error! FlushExecutor Await termination in close...");
            }
            if (!compactExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("Error! CompactExecutor Await termination in close...");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        synchronized (this) {
            if (tableStorage.isCompact()) {
                compact();
            }
            flush(memoryStorage);
        }
    }

    private SSTable flush(NavigableMap<ByteBuffer, Record> flushStorage) throws IOException {
        Path dir = config.dir;
        Path file = dir.resolve(SSTable.SSTABLE_FILE_PREFIX + tableCounter.getAndAdd(1));
        return SSTable.write(flushStorage.values().iterator(), file);
    }

    private Iterator<Record> sstableRanges(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(tableStorage.tables.size());
        for (SSTable ssTable : tableStorage.tables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return merge(iterators);
    }

    private SortedMap<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey,
                                              final NavigableMap<ByteBuffer, Record> storage) {
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

    /**
     * some doc
     */
    public static Iterator<Record> merge(List<Iterator<Record>> iterators) {
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

    public static Iterator<Record> mergeTwo(Iterator<Record> left, Iterator<Record> right) {
        return new MergeIterator(new PeekingIterator(left), new PeekingIterator(right));
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

    private class TableStorage {
        public final List<SSTable> tables;

        TableStorage() {
            tables = new CopyOnWriteArrayList<>();
        }

        TableStorage(final List<SSTable> newTables) {
            this.tables = newTables;
        }

        TableStorage(final SSTable table) {
            this(Collections.singletonList(table));
        }

        public TableStorage afterFlush(SSTable newTable) {
            List<SSTable> newTables = new CopyOnWriteArrayList<>();
            newTables.addAll(tables);
            newTables.add(newTable);
            return new TableStorage(newTables);
        }

        public TableStorage afterCompact(SSTable compactTable) {
            List<SSTable> newTables = new CopyOnWriteArrayList<>();
            // во время компакта, ещё флашились таблицы -> нужно их добавить
            for (int i = sizeBeforeCompact.get(); i < tables.size(); ++i) {
                newTables.add(tables.get(i));
            }
            newTables.add(compactTable);
            return new TableStorage(newTables);
        }

        public boolean isCompact() {
            return tables.size() >= config.tableLimit;
        }

    }

}
