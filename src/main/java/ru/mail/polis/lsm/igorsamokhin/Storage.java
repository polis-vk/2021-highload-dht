package ru.mail.polis.lsm.igorsamokhin;

import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

@SuppressWarnings("JdkObsolete")
@ThreadSafe
public final class Storage {
    public final MemTable currentMemTable;
    public final List<MemTable> memTablesToFlush;
    public final List<SSTable> ssTables;

    private Storage(MemTable currentMemTable, List<MemTable> memTablesToFlush, List<SSTable> ssTables) {
        this.currentMemTable = currentMemTable;
        this.memTablesToFlush = memTablesToFlush;
        this.ssTables = ssTables;
    }

    public static Storage init(List<SSTable> tables) {
        return new Storage(new MemTable(), Collections.emptyList(), tables);
    }

    public Storage prepareFlush() {
        ArrayList<MemTable> storagesToFlush = new ArrayList<>(this.memTablesToFlush.size() + 1);
        storagesToFlush.addAll(this.memTablesToFlush);
        storagesToFlush.add(currentMemTable);
        return new Storage(new MemTable(), storagesToFlush, ssTables);
    }

    public Storage afterFlush(List<MemTable> writtenStorage, SSTable newTable) {
        List<SSTable> newTables = new ArrayList<>(ssTables.size() + 1);
        newTables.addAll(ssTables);
        newTables.add(newTable);

        List<MemTable> newMemTablesToFlush = memTablesToFlush.subList(writtenStorage.size(),
                memTablesToFlush.size());
        return new Storage(currentMemTable, new ArrayList<>(newMemTablesToFlush), newTables);
    }

    public Storage afterCompaction(List<MemTable> writtenStorage, SSTable table) {
        List<MemTable> newMemTablesToFlush = memTablesToFlush.subList(writtenStorage.size(), memTablesToFlush.size());
        return new Storage(currentMemTable, new ArrayList<>(newMemTablesToFlush), Collections.singletonList(table));
    }

    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey, boolean includeTombstones) {
        List<Iterator<Record>> iterators = new ArrayList<>(ssTables.size());
        for (SSTable sstable : ssTables) {
            iterators.add(sstable.range(fromKey, toKey));
        }

        for (MemTable memTable : memTablesToFlush) {
            iterators.add(memTable.range(fromKey, toKey));
        }

        iterators.add(currentMemTable.range(fromKey, toKey));
        return merge(iterators, includeTombstones);
    }

    public Iterator<Record> flushIterator() {
        List<Iterator<Record>> iterators = new ArrayList<>(memTablesToFlush.size());

        for (MemTable memTable : memTablesToFlush) {
            iterators.add(memTable.range(null, null));
        }
        return merge(iterators, true);
    }

    public Iterator<Record> compactIterator() {
        List<Iterator<Record>> iterators = new ArrayList<>(ssTables.size());
        for (SSTable sstable : ssTables) {
            iterators.add(sstable.range(null, null));
        }
        return merge(iterators, true);
    }

    /**
     * Merge iterators into one iterator.
     */
    public static Iterator<Record> merge(List<Iterator<Record>> iterators, boolean includeTombstones) {
        return new MergingIterator(iterators, includeTombstones);
    }
}
