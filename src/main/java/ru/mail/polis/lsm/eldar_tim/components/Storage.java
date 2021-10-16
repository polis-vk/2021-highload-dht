package ru.mail.polis.lsm.eldar_tim.components;

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
        this(sstables,
                new ReadonlyMemTable(sstables.size()),
                new LimitedMemTable(sstables.size(), memoryTableSizeLimit));
    }

    public Storage stateReadyToFlush() {
        return new Storage(
                sstables,
                memTable.toReadOnly(),
                new LimitedMemTable(memTable.id + 1, memTable.memoryLimit)
        );
    }
}
