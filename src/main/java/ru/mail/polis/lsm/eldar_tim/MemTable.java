package ru.mail.polis.lsm.eldar_tim;

import ru.mail.polis.lsm.Record;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Описывает MemTable - страницу памяти в RAM.
 */
public class MemTable extends ConcurrentSkipListMap<ByteBuffer, Record> {
    /** Порядковый номер таблицы в памяти. */
    private final int id;

    public MemTable(int id) {
        super();
        this.id = id;
    }

    @Deprecated // FIXME
    public static MemTable newStorage(int id) {
        return new MemTable(id);
    }

    public int getId() {
        return id;
    }
}
