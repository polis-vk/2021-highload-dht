package ru.mail.polis.lsm.igorsamokhin;

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
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings("JdkObsolete")
public class LsmDAO implements DAO {
    private static final String FILE_PREFIX = "SSTable_";

    private final AtomicReference<SortedMap<ByteBuffer, Record>> flushingStorage = new AtomicReference<>(null);
    private final AtomicReference<SortedMap<ByteBuffer, Record>> memoryStorage =
            new AtomicReference<>(getNewStorage());

    private final List<SSTable> ssTables;
    private final DAOConfig config;
    private final AtomicInteger memoryConsumption = new AtomicInteger(0);

    private Path filePath;

    private ExecutorService executors = Executors.newCachedThreadPool();

    /**
     * Create DAO object.
     *
     * @param config - objects contains directory with data files
     */
    public LsmDAO(DAOConfig config) throws IOException {
        this.config = config;

        ssTables = SSTable.loadFromDir(config.dir);
        filePath = getNewFileName();
    }

    private Path getNewFileName() {
        String binary = Long.toBinaryString(ssTables.size());
        int leadingN = 64 - binary.length();

        String builder = "0".repeat(leadingN) + binary;
        String name = FILE_PREFIX.concat(builder);
        return config.dir.resolve(name);
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        Iterator<Record> memoryRange = SSTable.getSubMap(memoryStorage.get(), fromKey, toKey).values().iterator();
        Iterator<Record> sstableRanges = sstableRanges(fromKey, toKey);
        return LsmDAO.merge(List.of(sstableRanges, memoryRange));
    }

    @Override
    public void upsert(Record record) {
        int size = record.size();
        if (memoryConsumption.addAndGet(size) > config.memoryLimit) {
            synchronized (this) {
                if (memoryConsumption.get() > config.memoryLimit) {
                    Future<?> submit = executors.submit(flushTask(size));
                }
            }
        }

        memoryStorage.get().put(record.getKey(), record);
    }

    private Runnable flushTask(int initialSize) {
        return () -> {
            synchronized (memoryStorage){
                int prev = memoryConsumption.get();
                try {
                    flush(memoryStorage, flushingStorage, initialSize);
                } catch (IOException e) {
                    memoryConsumption.addAndGet(prev);
                    throw new UncheckedIOException(e);
                }
            }
        };
    }

    private ConcurrentSkipListMap<ByteBuffer, Record> getNewStorage() {
        return new ConcurrentSkipListMap<>();
    }

    private void flush(AtomicReference<SortedMap<ByteBuffer, Record>> oldStorage,
                       @Nullable AtomicReference<SortedMap<ByteBuffer, Record>> flushingStorage,
                       int initialMemoryConsumption) throws IOException {
        AtomicReference<SortedMap<ByteBuffer, Record>> tmpStorage = flushingStorage;
        if (tmpStorage == null) {
            tmpStorage = new AtomicReference<>(null);
        } else {
            if (tmpStorage.get() != null) {
                //finish previous flushing
                writeStorage(tmpStorage);
            }
            tmpStorage.set(getNewStorage());
        }

        SortedMap<ByteBuffer, Record> tmp = oldStorage.getAndSet(tmpStorage.get());
        tmpStorage.set(tmp);
        memoryConsumption.set(initialMemoryConsumption);

        writeStorage(tmpStorage);
        tmpStorage.set(null);
    }

    private void writeStorage(AtomicReference<SortedMap<ByteBuffer, Record>> storage) throws IOException {
        SSTable.write(storage.get().values().iterator(), filePath);
        SSTable ssTable = SSTable.loadFromFile(filePath);
        ssTables.add(ssTable);
        filePath = getNewFileName();
    }

    private Iterator<Record> sstableRanges(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(ssTables.size());
        for (SSTable sstable : ssTables) {
            iterators.add(sstable.range(fromKey, toKey));
        }
        return LsmDAO.merge(iterators);
    }

    @Override
    public void close() throws IOException {
        synchronized (memoryStorage) {
            flush(memoryStorage, null, Integer.MAX_VALUE);
            if (ssTables.isEmpty()) {
                return;
            }

            for (SSTable ssTable : ssTables) {
                ssTable.close();
            }
        }
    }

    @Override
    public void closeAndCompact() {
        synchronized (this) {
            Path compactFile = null;
            try {
                compactFile = SSTable.compact(config.dir, this.range(null, null));
            } catch (IOException e) {
                throw new UncheckedIOException("Can't compact", e);
            }

            if (compactFile == null) {
                return;
            }

            for (SSTable ssTable : ssTables) {
                try {
                    ssTable.close();
                } catch (IOException e) {
                    throw new UncheckedIOException("Can't close sstable", e);
                }
            }

            ssTables.clear();

            try {
                ssTables.addAll(SSTable.loadFromDir(config.dir));
            } catch (IOException e) {
                throw new UncheckedIOException("Can't load db from directory", e);
            }

            filePath = getNewFileName();
        }
    }

    /**
     * Merge iterators into one iterator.
     */
    public static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        return new MergingIterator(iterators);
    }
}
