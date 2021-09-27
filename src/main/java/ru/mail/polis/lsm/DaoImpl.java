package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;

public class DaoImpl implements DAO {

    private static final int MEMORY_LIMIT = 1024 * 1024 * 32;
    private NavigableMap<ByteBuffer, Record> map = new ConcurrentSkipListMap<>();
    private final ConcurrentLinkedDeque<SSTable> tables = new ConcurrentLinkedDeque<>();
    private int memoryConsumption;

    private final DAOConfig config;

    @GuardedBy("this")
    private int nextSStableIndex;

    /**
     * Implementation of DAO with Persistence.
     */
    public DaoImpl(DAOConfig config) throws IOException {
        this.config = config;
        List<SSTable> ssTables = SSTable.loadFromDir(config.getDir());
        nextSStableIndex = ssTables == null ? 0 : ssTables.size();
        tables.addAll(ssTables);
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return getRange(fromKey, toKey, true);
    }

    @Override
    public Iterator<Record> descendingRange(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return getRange(fromKey, toKey, false);
    }

    private Iterator<Record> getRange(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey, boolean isDirectOrder) {
        synchronized (this) {
            List<Iterator<Record>> iterators = new ArrayList<>(tables.size() + 1);
            for (SSTable ssTable : tables) {
                iterators.add(ssTable.range(fromKey, toKey, isDirectOrder));
            }

            Iterator<Record> memoryRange = map(fromKey, toKey, isDirectOrder).values().iterator();

            iterators.add(memoryRange);
            Iterator<Record> merged = merge(iterators, isDirectOrder);
            return new FilterIterator(merged,
                    isDirectOrder ? toKey : fromKey,
                    isDirectOrder);
        }
    }

    private SortedMap<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey,@Nullable ByteBuffer toKey,
                                              boolean isDirectOrder) {
        if ((fromKey == null) && (toKey == null)) {
            return isDirectOrder ? map : map.descendingMap();
        }

        if (fromKey == null) {
            return isDirectOrder ? map.headMap(toKey) : map.descendingMap().tailMap(toKey, false);
        }

        if (toKey == null) {
            return isDirectOrder ? map.tailMap(fromKey) : map.descendingMap().headMap(fromKey, true);
        }

        return isDirectOrder ? map.subMap(fromKey, toKey) :
                map.descendingMap().subMap(toKey, false, fromKey, true);
    }

    @Override
    public void upsert(Record record) {
        synchronized (this) {
            map.put(record.getKey(), record);
            memoryConsumption += record.getSize();
            if (memoryConsumption > MEMORY_LIMIT) {
                try {
                    flush();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    @Override
    public void closeAndCompact() {
        synchronized (this) {
            Iterator<Record> result = range(null, null);
            boolean dataExists = result.hasNext() && !tables.isEmpty();
            SSTable compactSSTable = null;
            String compactFileName = "file_" + tables.size();
            String compactIdxName = "idx_" + tables.size();
            try {
                if (dataExists) {
                    compactSSTable = makeSSTable(compactFileName, compactIdxName, result);
                    tables.add(compactSSTable);
                }
                removeOldSSTables();
                if (dataExists) {
                    map.clear();
                    Files.move(config.getDir().resolve(compactFileName), config.getDir().resolve("file_0"));
                    Files.move(config.getDir().resolve(compactIdxName), config.getDir().resolve("idx_0"));
                    Objects.requireNonNull(compactSSTable).setFilePath(config.getDir().resolve("file_0"));
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Can't compact", e);
            }
        }
    }

    private void removeOldSSTables() throws IOException {
        if (tables.isEmpty()) {
            return;
        }

        String compactionFileNumber = tables.getLast().getFilePath().getFileName().toString().substring(5);
        for (SSTable table : tables) {
            if (!table.getFilePath().getFileName().toString().contains(compactionFileNumber)) {
                table.close();
                tables.remove(table);
                Files.deleteIfExists(table.getFilePath());
                Files.deleteIfExists(table.getIdxPath());
            }
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            flush();
            for (SSTable table : tables) {
                table.close();
            }
        }
    }

    @GuardedBy("this")
    private void flush() throws IOException {
        Iterator<Record> data = map.values().iterator();
        if (memoryConsumption > 0 && data.hasNext()) {
            tables.add(makeSSTable(
                    "file_" + nextSStableIndex,
                    "idx_" + nextSStableIndex,
                    data
            ));

            nextSStableIndex++;
            memoryConsumption = 0;
            map = new ConcurrentSkipListMap<>();
        }
    }

    private SSTable makeSSTable(String fileName, String idxName, Iterator<Record> data) throws IOException {
        Path dir = config.getDir();
        Path file = dir.resolve(fileName);
        Path idx = dir.resolve(idxName);
        return SSTable.write(data, file, idx);
    }

    private Iterator<Record> merge(List<Iterator<Record>> iterators, boolean isDirectOrder) {
        return new MergeIterator(iterators, isDirectOrder);
    }

}
