package ru.mail.polis.lsm.artemdrozdov;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TableStorage {
    public final List<SSTable> tables;
    private static Lock compactLock = new ReentrantLock();

    TableStorage(final List<SSTable> newTables) {
        this.tables = newTables;
    }

    TableStorage(final SSTable table) {
        this(Collections.singletonList(table));
    }

    /**
     * some doc.
     */
    public TableStorage afterFlush(SSTable newTable) {
        compactLock.lock();
        List<SSTable> newTables;
        try {
            newTables = new CopyOnWriteArrayList<>();
            newTables.addAll(tables);
            newTables.add(newTable);
        } finally {
            compactLock.unlock();
        }

        return new TableStorage(newTables);
    }

    /**
     * some doc.
     */
    public TableStorage afterCompact(SSTable compactTable, final int sizeBeforeCompact) {
        compactLock.lock();
        List<SSTable> newTables;
        try {
            newTables = new CopyOnWriteArrayList<>();
            // во время компакта, ещё флашились таблицы -> нужно их добавить
            for (int i = sizeBeforeCompact; i < tables.size(); ++i) {
                newTables.add(tables.get(i));
            }
            newTables.add(compactTable);
        } finally {
            compactLock.unlock();
        }

        return new TableStorage(newTables);
    }

    public boolean isCompact(final int tableLimit) {
        return tables.size() >= tableLimit;
    }
}
