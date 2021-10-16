package ru.mail.polis.lsm.eldar_tim.components;

import java.util.ArrayList;
import java.util.List;

public class Storage {

    public final List<SSTable> sstables;
    public final MemTable memTableToFlush;
    public final LimitedMemTable memTable;

    public Storage(List<SSTable> sstables, MemTable memoryTableToFlush, LimitedMemTable memoryTable) {
        this.sstables = sstables;
        this.memTableToFlush = memoryTableToFlush;
        this.memTable = memoryTable;
    }

    public Storage(List<SSTable> sstables, int memoryTableSizeLimit) {
        this(sstables, ReadonlyMemTable.BLANK, new LimitedMemTable(sstables.size(), memoryTableSizeLimit));
    }

    public Storage beforeFlush() {
        return new Storage(
                sstables,
                memTable.toReadOnly(),
                new LimitedMemTable(memTable.getId() + 1, memTable.memoryLimit)
        );
    }

    public Storage afterFlush(SSTable flushedTable) {
        List<SSTable> newTables = new ArrayList<>(sstables.size() + 1);
        newTables.addAll(sstables);
        newTables.add(flushedTable);
        return new Storage(newTables, ReadonlyMemTable.BLANK, memTable);
    }
}
