package ru.mail.polis.lsm.alyonazakharova;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LmsDAO implements DAO {

    private NavigableMap<ByteBuffer, Record> memoryStorage = new ConcurrentSkipListMap<>();
    private final ConcurrentLinkedDeque<SSTable> ssTables = new ConcurrentLinkedDeque<>();

    @SuppressWarnings("unused")
    private final DAOConfig config;

//    @GuardedBy("this")
    private int nextSSTableIndex;

    @GuardedBy("this")
    private int memoryConsumption;

    /**
     * Init DAO.
     *
     * @param config is used to get the file directory
     * @throws IOException if an I/O error occurs while opening FileChannel
     */
    public LmsDAO(DAOConfig config) throws IOException {
        this.config = config;
        List<SSTable> existingSSTables = SSTable.loadFromDir(config.dir);
        nextSSTableIndex = existingSSTables.size();
        ssTables.addAll(existingSSTables);
    }

    @Override
    public Iterator<Record> range(@Nullable final ByteBuffer fromKey, @Nullable final ByteBuffer toKey) {
        synchronized (this) {
            try {
                Iterator<Record> ssTableRanges = ssTableRanges(fromKey, toKey);
                Iterator<Record> memoryRange = map(fromKey, toKey).values().iterator();
                Iterator<Record> iterator = mergeTwo(
                        new PeekingIterator(ssTableRanges),
                        new PeekingIterator(memoryRange)
                );
                return filterTombstones(iterator);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public void upsert(final Record record) {
        synchronized (this) {
            memoryConsumption += sizeOf(record);
            if (memoryConsumption > config.memoryLimit) {
                try {
                    flush();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                memoryConsumption = sizeOf(record);
            }
        }
        memoryStorage.put(record.getKey(), record);
    }

    @Override
    public void closeAndCompact() {
        Iterator<Record> iterator = range(null, null);
        Path dir = config.dir;

        try {
            List<SSTable> compactedSSTables = SSTable.writeCompacted(dir, iterator);

            try (Stream<Path> stream = Files.list(dir)) {
                List<Path> files = stream
                        .filter(file -> !file.toFile().getName().contains(SSTable.COMPACTED_FILE_PREFIX))
                        .collect(Collectors.toList());

                closeTables();

                for (Path file : files) {
                    Files.delete(file);
                }
            }

            ssTables.clear();
            ssTables.addAll(compactedSSTables);

            for (int i = 0; i < compactedSSTables.size(); i++) {
                Path newFile = dir.resolve(SSTable.SSTABLE_FILE_PREFIX + i);
                Path newIndexFile = SSTable.getIndexFile(newFile);
                Path oldFile = compactedSSTables.get(i).getFile();
                Path oldIndexFile = SSTable.getIndexFile(oldFile);
                try {
                    Files.move(oldFile, newFile, StandardCopyOption.ATOMIC_MOVE);
                    Files.move(oldIndexFile, newIndexFile, StandardCopyOption.ATOMIC_MOVE);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            nextSSTableIndex = compactedSSTables.size() + 1;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            flush();
        }
        closeTables();
    }

    private void closeTables() throws IOException {
        for (SSTable sstable : ssTables) {
            sstable.close();
        }
    }

    @GuardedBy("this")
    private void flush() throws IOException {
        Path dir = config.dir;
        Path file = dir.resolve("file_" + nextSSTableIndex);
        nextSSTableIndex++;

        SSTable ssTable = SSTable.write(file, memoryStorage.values().iterator());
        ssTables.add(ssTable);
        memoryStorage = new ConcurrentSkipListMap<>();
    }

    private int sizeOf(Record record) {
        return SSTable.sizeOf(record);
    }

    private Iterator<Record> ssTableRanges(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) throws IOException {
        List<Iterator<Record>> iterators = new ArrayList<>(ssTables.size());
        for (SSTable ssTable : ssTables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return merge(iterators);
    }

    private SortedMap<ByteBuffer, Record> map(@Nullable final ByteBuffer fromKey, @Nullable final ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return memoryStorage;
        } else if (fromKey == null) {
            return memoryStorage.headMap(toKey);
        } else if (toKey == null) {
            return memoryStorage.tailMap(fromKey);
        }
        return memoryStorage.subMap(fromKey, toKey);
    }

    private static class PeekingIterator implements Iterator<Record> {

        private Record current;
        private final Iterator<Record> delegate;

        public PeekingIterator(Iterator<Record> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return current != null || delegate.hasNext();
        }

        @Override
        public Record next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Record now = peek();
            current = null;
            return now;
        }

        public Record peek() {
            if (current != null) {
                return current;
            }
            if (!hasNext()) {
                return null;
            }
            current = delegate.next();
            return current;
        }
    }

    /**
     * Merges list of sorted iterators.
     *
     * @param iterators is list of sorted iterators
     * @return iterator over sorted records
     */
    private static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        int size = iterators.size();
        if (size == 0) {
            return Collections.emptyIterator();
        } else if (size == 1) {
            return iterators.get(0);
        } else if (size == 2) {
            return mergeTwo(new PeekingIterator(iterators.get(0)), new PeekingIterator(iterators.get(1)));
        } else {
            int middle = size / 2;
            Iterator<Record> left = merge(iterators.subList(0, middle));
            Iterator<Record> right = merge(iterators.subList(middle, size));
            return mergeTwo(new PeekingIterator(left), new PeekingIterator(right));
        }
    }

    /**
     * Merges two sorted iterators.
     *
     * @param left  has lower priority
     * @param right has higher priority
     * @return iterator over merged sorted records
     */
    private static Iterator<Record> mergeTwo(PeekingIterator left, PeekingIterator right) {
        return new Iterator<>() {

            @Override
            public boolean hasNext() {
                return left.hasNext() || right.hasNext();
            }

            @Override
            public Record next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("No elements");
                }

                if (!left.hasNext()) {
                    return right.next();
                }
                if (!right.hasNext()) {
                    return left.next();
                }

                ByteBuffer leftKey = left.peek().getKey();
                ByteBuffer rightKey = right.peek().getKey();

                int compareResult = leftKey.compareTo(rightKey);
                if (compareResult == 0) {
                    left.next();
                    return right.next();
                }
                if (compareResult < 0) {
                    return left.next();
                } else {
                    return right.next();
                }
            }
        };
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
