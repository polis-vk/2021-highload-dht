package ru.mail.polis.lsm.artemdrozdov;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class TableStorage {
    public final List<SSTable> tables;

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
        List<SSTable> newTables = new CopyOnWriteArrayList<>();
        newTables.addAll(tables);
        newTables.add(newTable);
        return new TableStorage(newTables);
    }

    /**
     * some doc.
     */
    public TableStorage afterCompact(SSTable compactTable, final int sizeBeforeCompact) {
        List<SSTable> newTables = new CopyOnWriteArrayList<>();
        // во время компакта, ещё флашились таблицы -> нужно их добавить
        for (int i = sizeBeforeCompact; i < tables.size(); ++i) {
            newTables.add(tables.get(i));
        }
        newTables.add(compactTable);
        return new TableStorage(newTables);
    }

    public boolean isCompact(final int tableLimit) {
        return tables.size() >= tableLimit;
    }
}
