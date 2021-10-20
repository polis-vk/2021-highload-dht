package ru.mail.polis.lsm.artem_drozdov;

import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class MemStorage {

    public final MemTable currentMemTable;
    public final List<MemTable> memTablesToWrite;
    public final List<SSTable> ssTableList;

    public static MemStorage init(List<SSTable> tableList) {
        return new MemStorage(new MemTable(), Collections.emptyList(), tableList);
    }

    private MemStorage(MemTable currentMemTable, List<MemTable> memTablesToWrite, List<SSTable> ssTableList) {
        this.currentMemTable = currentMemTable;
        this.memTablesToWrite = memTablesToWrite;
        this.ssTableList = ssTableList;
    }

    public MemStorage prepareFlush() {
        List<MemTable> tablesToFlush = new ArrayList<>(memTablesToWrite.size() + 1);
        tablesToFlush.addAll(memTablesToWrite);
        tablesToFlush.add(currentMemTable);
        return new MemStorage(new MemTable(), tablesToFlush, ssTableList);
    }

    // it is assumed that memTablesToWrite start with writtenStorages
    public MemStorage afterFlush(List<MemTable> writtenStorages, SSTable ssTable) {
        List<SSTable> tableList = new ArrayList<>(this.ssTableList.size() + 1);
        tableList.addAll(this.ssTableList);
        tableList.add(ssTable);

        List<MemTable> newMemTablesToFlush = memTablesToWrite.subList(writtenStorages.size(), memTablesToWrite.size());
        return new MemStorage(currentMemTable, new ArrayList<>(newMemTablesToFlush), tableList);
    }

    // it is assumed that memTablesToWrite start with writtenStorages
    public MemStorage afterCompaction(List<MemTable> writtenStorages, SSTable ssTable) {
        List<MemTable> newMemTablesToFlush = memTablesToWrite.subList(writtenStorages.size(), memTablesToWrite.size());
        return new MemStorage(currentMemTable, new ArrayList<>(newMemTablesToFlush), Collections.singletonList(ssTable));
    }

    public Iterator<Record> iterator(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>();
        // order is mater - older data first
        for (SSTable ssTable : ssTableList) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        for (MemTable memTable : memTablesToWrite) {
            iterators.add(memTable.range(fromKey, toKey));
        }
        iterators.add(currentMemTable.range(fromKey, toKey));
        return merge(iterators);
    }

    public Iterator<Record> flushIterator() {
        List<Iterator<Record>> iterators = new ArrayList<>();
        // order is mater - older data first
        for (MemTable memTable : memTablesToWrite) {
            iterators.add(memTable.range(null, null));
        }
        return merge(iterators);
    }

    public Iterator<Record> ssTableIterator(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(ssTableList.size());
        for (SSTable ssTable : ssTableList) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return merge(iterators);
    }

    private static Iterator<Record> merge(List<Iterator<Record>> iterators) {
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
}
