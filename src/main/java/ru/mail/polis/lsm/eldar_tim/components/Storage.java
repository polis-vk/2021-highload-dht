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

    public Storage afterCompact(List<SSTable> outdatedList, SSTable compacted) {
        List<SSTable> newTables = new ArrayList<>(sstables.size() - outdatedList.size() + 1);

        int sstablesSize = sstables.size();

        int indexL = sstables.indexOf(outdatedList.get(0));
        int indexR = sstables.indexOf(outdatedList.get(outdatedList.size() - 1));

        List<SSTable> sublistL = sstables.subList(0, indexL);
        List<SSTable> sublistR = sstables.subList(indexR + 1, sstablesSize);

        newTables.addAll(sublistL);
        newTables.add(compacted);
        newTables.addAll(sublistR);

        return new Storage(newTables, memTableToFlush, memTable);
    }
}
