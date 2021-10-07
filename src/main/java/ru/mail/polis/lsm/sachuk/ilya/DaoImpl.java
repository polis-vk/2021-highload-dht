package ru.mail.polis.lsm.sachuk.ilya;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.sachuk.ilya.iterators.MergeIterator;
import ru.mail.polis.lsm.sachuk.ilya.iterators.PeekingIterator;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class DaoImpl implements DAO {

    private final Path dirPath;
    private final DAOConfig config;
    private NavigableMap<ByteBuffer, Record> memoryStorage = new ConcurrentSkipListMap<>();
    private final List<SSTable> ssTables = new ArrayList<>();
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final AtomicInteger counter = new AtomicInteger(0);
    private final ConcurrentLinkedQueue<Iterator> queue = new ConcurrentLinkedQueue<>();
    private final Object object = new Object();
    private NavigableMap<ByteBuffer, Record> tmpStorageForFlush = new ConcurrentSkipListMap<>();

    private final AtomicInteger memoryConsumption = new AtomicInteger();
    private final AtomicInteger nextSSTableNumber = new AtomicInteger();

    /**
     * Constructor that initialize path and restore storage.
     *
     * @param config is config.
     * @throws IOException is thrown when an I/O error occurs.
     */
    public DaoImpl(DAOConfig config) throws IOException {
        this.config = config;
        this.dirPath = config.dir;

        ssTables.addAll(SSTable.loadFromDir(dirPath));
        nextSSTableNumber.set(ssTables.size());
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        Iterator<Record> ssTableRanges = ssTableRanges(fromKey, toKey);

        Iterator<Record> memoryRange = map(fromKey, toKey).values().iterator();
        Iterator<Record> mergedIterators = mergeTwo(ssTableRanges, memoryRange);

        return StreamSupport
                .stream(
                        Spliterators.spliteratorUnknownSize(mergedIterators, Spliterator.ORDERED),
                        false
                )
                .filter(record -> !record.isTombstone())
                .iterator();
    }

    @Override
    public void upsert(Record record) {
        if (memoryConsumption.addAndGet(sizeOf(record)) > config.memoryLimit) {
            synchronized (this) {
                if (memoryConsumption.get() > config.memoryLimit) {
                    int prev = memoryConsumption.getAndSet(sizeOf(record));

                    tmpStorageForFlush.putAll(memoryStorage);
                    memoryStorage = new ConcurrentSkipListMap<>();

                    queue.add(tmpStorageForFlush.values().iterator());
                    executorService.submit(() -> prepareAndFlush(prev));

                    while (queue.size() > 3) {

                    }
                }
            }
        }
        memoryStorage.put(record.getKey(), record);
    }

    @Override
    public void closeAndCompact() {
        synchronized (this) {
            Iterator<Record> iterator = range(null, null);

            try {
                SSTable compactedTable = SSTable.save(iterator, dirPath, nextSSTableNumber.getAndIncrement());

                String indexFile = compactedTable.getIndexPath().getFileName().toString();
                String saveFile = compactedTable.getSavePath().getFileName().toString();

                closeOldTables(indexFile, saveFile);
                deleteOldTables(indexFile, saveFile);

                ssTables.add(compactedTable);

                Files.move(compactedTable.getIndexPath(),
                        dirPath.resolve(SSTable.FIRST_INDEX_FILE),
                        StandardCopyOption.ATOMIC_MOVE
                );

                Files.move(compactedTable.getSavePath(),
                        dirPath.resolve(SSTable.FIRST_SAVE_FILE),
                        StandardCopyOption.ATOMIC_MOVE
                );
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public void close() throws IOException {

        while (counter.get() != 0) {

        }

        if (memoryConsumption.get() > 0) {
            flush();
        }

        closeSSTables();
    }

    private void closeSSTables() throws IOException {
        for (SSTable ssTable : ssTables) {
            ssTable.close();
        }
    }

    private Map<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {

        if (fromKey == null && toKey == null) {
            return memoryStorage;
        } else if (fromKey == null) {
            return memoryStorage.headMap(toKey);
        } else if (toKey == null) {
            return memoryStorage.tailMap(fromKey);
        } else {
            return memoryStorage.subMap(fromKey, toKey);
        }
    }

    private void prepareAndFlush(int prev) {
        try {
            flush();
        } catch (IOException e) {
            memoryConsumption.addAndGet(prev);
            throw new UncheckedIOException(e);
        }
    }

    private void flush() throws IOException {
        counter.addAndGet(1);

        synchronized (object) {

            Iterator iterator;
            if (tmpStorageForFlush.isEmpty() && queue.isEmpty()) {
                tmpStorageForFlush.putAll(memoryStorage);
                iterator = tmpStorageForFlush.values().iterator();
            } else {
                iterator = queue.poll();
            }

            SSTable ssTable = SSTable.save(
                    iterator,
                    dirPath,
                    nextSSTableNumber.getAndIncrement()
            );

            ssTables.add(ssTable);
            tmpStorageForFlush = new ConcurrentSkipListMap<>();
            if (counter.get() != 0) {
                counter.decrementAndGet();

            }
        }
    }

    private Iterator<Record> ssTableRanges(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(ssTables.size());

        for (SSTable ssTable : ssTables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return merge(iterators);
    }

    private int sizeOf(Record record) {
        return record.getKey().remaining()
                + (record.isTombstone() ? 0 : record.getKey().remaining()) + Integer.BYTES * 2;
    }

    private void closeOldTables(String indexFile, String saveFile) throws IOException {
        List<SSTable> filteredSSTables = ssTables.stream()
                .filter(ssTable1 -> checkFilesNameEquals(ssTable1.getIndexPath(), indexFile)
                        && checkFilesNameEquals(ssTable1.getSavePath(), saveFile))
                .collect(Collectors.toList());

        for (SSTable filteredSSTable : filteredSSTables) {
            filteredSSTable.close();
        }
    }

    private void deleteOldTables(String indexFile, String saveFile) throws IOException {
        try (Stream<Path> paths = Files.walk(dirPath)) {
            paths.filter(Files::isRegularFile)
                    .map(Path::toFile)
                    .filter(file -> file.getName().compareTo(indexFile) != 0 && file.getName().compareTo(saveFile) != 0)
                    .forEach(File::delete);
        }

        ssTables.clear();
    }

    private boolean checkFilesNameEquals(Path filePath, String fileToCompare) {
        return filePath.toString().compareTo(fileToCompare) != 0;
    }

    /**
     * Method that merge iterators and return iterator.
     *
     * @param iterators is list of iterators to merge
     * @return merged iterators
     */
    public static Iterator<Record> merge(List<Iterator<Record>> iterators) {

        if (iterators.isEmpty()) {
            return Collections.emptyIterator();
        } else if (iterators.size() == 1) {
            return iterators.get(0);
        } else if (iterators.size() == 2) {
            return mergeTwo(iterators.get(0), iterators.get(1));
        }

        Iterator<Record> left = merge(iterators.subList(0, iterators.size() / 2));
        Iterator<Record> right = merge(iterators.subList(iterators.size() / 2, iterators.size()));

        return mergeTwo(left, right);
    }

    private static Iterator<Record> mergeTwo
            (Iterator<Record> leftIterator, Iterator<Record> rightIterator) {
        return new MergeIterator(new PeekingIterator<>(leftIterator), new PeekingIterator<>(rightIterator));
    }
}

