package ru.mail.polis.lsm.eldar_tim;

import ru.mail.polis.lsm.DAOConfig;

import java.util.List;

public class Storage {

    final List<SSTable> sstables;
    final MemTable memTableToFlush;
    final LimitedMemTable memTable;

    public Storage(List<SSTable> sstables, MemTable memoryTableToFlush, LimitedMemTable memoryTable) {
        this.sstables = sstables;
        this.memTableToFlush = memoryTableToFlush;
        this.memTable = memoryTable;
    }

    public Storage(List<SSTable> sstables, int memoryTableSizeLimit) {
        this(sstables, new MemTable(sstables.size()), new LimitedMemTable(sstables.size(), memoryTableSizeLimit));
    }
}
