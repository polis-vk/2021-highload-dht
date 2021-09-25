package ru.mail.polis.lsm.ilia;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;

public class DAOImpl implements DAO {

    private static final String COMPACT = "COMPACT";
    private static final String FILE_NAME_COMPACT = SSTable.SSTABLE_FILE_PREFIX + COMPACT;
    private static final String FILE_NAME_COMPACT_RESULT = SSTable.SSTABLE_FILE_PREFIX + "0";
    private static final String IDX = ".idx";

    private SortedMap<ByteBuffer, Record> memoryStorage = new ConcurrentSkipListMap<>();
    private final ConcurrentLinkedDeque<SSTable> tables = new ConcurrentLinkedDeque<>();

    private final DAOConfig config;

    private int nextSStableIndex;
    private int memorySize;

    /**
     * Create DAOImpl constructor.
     *
     * @param config contains directory with file
     */
    public DAOImpl(DAOConfig config) throws IOException {
        this.config = config;
        List<SSTable> ssTables = SSTable.loadFromDir(config.dir);
        nextSStableIndex = ssTables.size();
        tables.addAll(ssTables);
    }

    /**
     * Merges iterators.
     *
     * @param iterators iterators List
     * @return result Iterator
     */
    private static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        switch (iterators.size()) {
            case (0):
                return Collections.emptyIterator();
            case (1):
                return iterators.get(0);
            case (2):
                return new MergeTwo(new PeekingIterator(iterators.get(0)), new PeekingIterator(iterators.get(1)));
            default:
                Iterator<Record> left = merge(iterators.subList(0, iterators.size() / 2));
                Iterator<Record> right = merge(iterators.subList(iterators.size() / 2, iterators.size()));

                return new MergeTwo(new PeekingIterator(left), new PeekingIterator(right));
        }
    }

    @Override
    public void closeAndCompact() throws IOException {
        synchronized (this) {
            Iterator<Record> result = new PeekingIterator(range(null, null));
            Path dir = config.dir;

            if (result.hasNext()) {
                SSTable ssTable = SSTable.write(result, dir.resolve(DAOImpl.FILE_NAME_COMPACT));
                tables.add(ssTable);

                for (SSTable table : tables) {
                    if (!table.getPath().toString().endsWith(COMPACT)) {
                        table.close();
                        Path path = table.getPath();
                        Files.deleteIfExists(path);
                        Files.deleteIfExists(Paths.get(path + IDX));
                        tables.remove(table);
                    }
                }

                memoryStorage.clear();
                Files.move(
                        dir.resolve(FILE_NAME_COMPACT),
                        dir.resolve(FILE_NAME_COMPACT_RESULT),
                        StandardCopyOption.ATOMIC_MOVE
                );
                Files.move(
                        dir.resolve(FILE_NAME_COMPACT + IDX),
                        dir.resolve(FILE_NAME_COMPACT_RESULT + IDX),
                        StandardCopyOption.ATOMIC_MOVE
                );
            }
        }
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        synchronized (this) {
            Iterator<Record> ssTableRanges = sstableRanges(fromKey, toKey);
            Iterator<Record> memoryRange = map(fromKey, toKey).values().iterator();
            Iterator<Record> mergeTwo = new MergeTwo(
                    new PeekingIterator(ssTableRanges),
                    new PeekingIterator(memoryRange)
            );
            return new FilterIterator(mergeTwo);
        }
    }

    @Override
    public void upsert(@Nonnull Record record) {
        synchronized (this) {
            memorySize += SSTable.sizeOf(record);
            if (memorySize > config.memoryLimit) {
                try {
                    flush();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            memoryStorage.put(record.getKey(), record);
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (!memoryStorage.isEmpty()) {
                flush();
            }
        }
    }

    private void flush() throws IOException {
        Path dir = config.dir;
        Path file = dirResolve(dir);

        SSTable ssTable = SSTable.write(memoryStorage.values().iterator(), file);
        tables.add(ssTable);
        memoryStorage = new ConcurrentSkipListMap<>();
        memorySize = 0;
    }

    private Path dirResolve(Path dir) {
        Path file = dir.resolve(SSTable.SSTABLE_FILE_PREFIX + nextSStableIndex);
        nextSStableIndex++;
        return file;
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
}
