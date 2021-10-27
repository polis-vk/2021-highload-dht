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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class LsmDAO implements DAO {

    private static final Logger LOG = LoggerFactory.getLogger(LsmDAO.class);
    private final AtomicReference<Storage> storage;

    private final DAOConfig config;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public LsmDAO(DAOConfig config) throws IOException {
        this.config = config;
        storage = new AtomicReference<>(Storage.init(SSTable.loadFromDir(config.dir)));
    }

    public static Iterator<Record> merge(List<Iterator<Record>> iterators) {
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
        Storage tmpStorage = this.storage.get();

        Iterator<Record> sstableRanges = sstableRanges(tmpStorage, fromKey, toKey);
        Iterator<Record> memoryRange = tmpStorage.iterator(fromKey, toKey);

        Iterator<Record> iterator =
                new RecordMergingIterator(
                        new PeekingIterator<>(sstableRanges),
                        new PeekingIterator<>(memoryRange));
        return new TombstoneFilteringIterator(iterator);
    }

    @Override
    public void upsert(Record record) {
        Storage tmpStorage = this.storage.get();
        long consumption = tmpStorage.memoryStorage.put(record);

        if (consumption > config.memoryLimit) {
            boolean success = this.storage.compareAndSet(tmpStorage, tmpStorage.swap());
            if (!success) {
                return; //another thread updated the storage
            }
            LOG.debug("Going to flush {}", consumption);

            if (!tmpStorage.memTablesToFlush.isEmpty()) {
                return;
            }

            executor.execute(() -> {
                asyncFlushAndCompact(tmpStorage);
            });
        }
    }

    private void asyncFlushAndCompact(Storage storage) {
        try {
            LOG.debug("Flushing");
            while (true) {
                Storage storageToFlush = this.storage.get();
                List<MemTable> memTables = storageToFlush.memTablesToFlush;
                if (memTables.isEmpty()) {
                    break;
                }
                SSTable newTable = flushAll(storageToFlush);

                this.storage.updateAndGet(current -> current.restore(memTables, newTable));
            }
            if (storage.ssTables.size() > config.maxTables) {
                doCompact();
            }
            LOG.debug("Flushed");

        } catch (IOException e) {
            LOG.error("Can't flush");
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void compact() {
        executor.execute(() -> {
            synchronized (this) {
                try {
                    doCompact();
                } catch (IOException e) {
                    LOG.error("Can't compact", e);
                    throw new UncheckedIOException(e);
                }
            }
        });
    }

    private void doCompact() throws IOException {
        SSTable result = SSTable.compact(config.dir, sstableRanges(storage.get(), null, null));
        this.storage.updateAndGet(current -> current.endCompact(storage.get().memTablesToFlush, result));
    }

    @Override
    public void close() throws IOException {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                throw new IllegalStateException("Can't await for termination");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        flushAll(storage.get().swap());
    }

    private SSTable flushAll(Storage storage) throws IOException {
        Path dir = config.dir;
        Path file = dir.resolve(SSTable.SSTABLE_FILE_PREFIX + storage.ssTables.size());

        return SSTable.write(storage.flushIterator(), file);
    }

    private Iterator<Record> sstableRanges(Storage storage, @Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(storage.ssTables.size());
        for (SSTable ssTable : storage.ssTables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return merge(iterators);
    }
}
