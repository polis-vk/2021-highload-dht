package ru.mail.polis.lsm.artem_drozdov;

import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public final class Storage {
    public MemTable memoryStorage;
    public List<MemTable> memTablesToFlush;
    public final List<SSTable> ssTables;

    private Storage(MemTable memoryStorage,
                    List<MemTable> memTablesToFlush,
                    List<SSTable> ssTables) {
        this.memoryStorage = memoryStorage;
        this.memTablesToFlush = memTablesToFlush;
        this.ssTables = ssTables;
    }

    public static Storage init(List<SSTable> tables) {
        return new Storage(new MemTable(), Collections.emptyList(), tables);
    }

    public Storage swap() {
        List<MemTable> storageToFlush = new ArrayList<>(this.memTablesToFlush.size() + 1);
        storageToFlush.addAll(this.memTablesToFlush);
        storageToFlush.add(memoryStorage);
        return new Storage(new MemTable(), storageToFlush, ssTables);
    }

    public Storage restore(List<MemTable> writtenMemTables, SSTable newTable) {
        List<SSTable> newTables = new ArrayList<>(ssTables.size() + 1);
        newTables.addAll(ssTables);
        newTables.add(newTable);
        List<MemTable> newMemTablesToFlush = memTablesToFlush.subList(writtenMemTables.size(), memTablesToFlush.size());
        return new Storage(memoryStorage, new ArrayList<>(newMemTablesToFlush), newTables);
    }

    public Storage endCompact(List<MemTable> memTables, SSTable table) {
        List<MemTable> newMemTablesToFlush = memTablesToFlush.subList(memTables.size(), memTablesToFlush.size());
        return new Storage(memoryStorage, new ArrayList<>(newMemTablesToFlush), Collections.singletonList(table));
    }

    public Iterator<Record> iterator(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>();
        for (SSTable table : ssTables) {
            iterators.add(table.range(fromKey, toKey));
        }
        for (MemTable memTable : memTablesToFlush) {
            iterators.add(memTable.range(fromKey, toKey));
        }
        iterators.add(memoryStorage.range(fromKey, toKey));
        return LsmDAO.merge(iterators);
    }

    public Iterator<Record> flushIterator() {
        List<Iterator<Record>> iterators = new ArrayList<>();
        for (MemTable memTable : memTablesToFlush) {
            iterators.add(memTable.range(null, null));
        }
        return LsmDAO.merge(iterators);
    }
}

