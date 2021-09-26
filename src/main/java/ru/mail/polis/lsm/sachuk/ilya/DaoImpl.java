package ru.mail.polis.lsm.sachuk.ilya;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.sachuk.ilya.iterators.MergeIterator;

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
import java.util.SortedMap;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class DaoImpl implements DAO {

    private static final long LIMIT = 16L * 1024 * 1024;

    private final Path dirPath;
    private final SortedMap<ByteBuffer, Record> memoryStorage = new ConcurrentSkipListMap<>();
    private final List<SSTable> ssTables = new ArrayList<>();

    private long memoryConsumption;
    private int nextSSTableNumber;

    /**
     * Constructor that initialize path and restore storage.
     *
     * @param config is config.
     * @throws IOException is thrown when an I/O error occurs.
     */
    public DaoImpl(DAOConfig config) throws IOException {
        this.dirPath = config.dir;

        ssTables.addAll(SSTable.loadFromDir(dirPath));
        nextSSTableNumber = ssTables.size();
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        synchronized (this) {
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
    }

    @Override
    public void upsert(Record record) throws UncheckedIOException {
        synchronized (this) {
            memoryConsumption += sizeOf(record);
            if (memoryConsumption > LIMIT) {
                try {
                    flush();
                    memoryConsumption = 0;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }

        memoryConsumption += sizeOf(record);
        memoryStorage.put(record.getKey(), record);
    }

    @Override
    public void closeAndCompact() {
        synchronized (this) {
            Iterator<Record> iterator = range(null, null);

            try {
                SSTable compactedTable = SSTable.save(iterator, dirPath, nextSSTableNumber++);

                String indexFile = compactedTable.getIndexPath().getFileName().toString();
                String saveFile = compactedTable.getSavePath().getFileName().toString();


                closeOldTables(indexFile, saveFile);
                deleteOldTables(indexFile, saveFile);

                ssTables.add(compactedTable);

                Files.move(compactedTable.getIndexPath(), dirPath.resolve(SSTable.FIRST_INDEX_FILE), StandardCopyOption.ATOMIC_MOVE);
                Files.move(compactedTable.getSavePath(), dirPath.resolve(SSTable.FIRST_SAVE_FILE), StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public void close() throws IOException {

        if (memoryConsumption > 0) {
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

    private void flush() throws IOException {
        SSTable ssTable = SSTable.save(
                memoryStorage.values().iterator(),
                dirPath,
                nextSSTableNumber++
        );

        ssTables.add(ssTable);
        memoryStorage.clear();

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
                .filter(ssTable1 -> checkFilesNameEquals(ssTable1.getIndexPath(), indexFile) &&
                        checkFilesNameEquals(ssTable1.getSavePath(), saveFile))
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

    private static Iterator<Record> mergeTwo(Iterator<Record> leftIterator, Iterator<Record> rightIterator) {
        return new MergeIterator(leftIterator, rightIterator);
    }
}

