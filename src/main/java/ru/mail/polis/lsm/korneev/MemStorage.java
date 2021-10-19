package ru.mail.polis.lsm.korneev;

import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.artem_drozdov.SSTable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemStorage {

    private static final NavigableMap<ByteBuffer, Record> EMPTY_STORAGE = Collections.emptyNavigableMap();

    public final NavigableMap<ByteBuffer, Record> currentStorage;
    public final NavigableMap<ByteBuffer, Record> storageToWrite;
    public final List<SSTable> tableList;

    public static MemStorage init(List<SSTable> tableList) {
        return new MemStorage(new ConcurrentSkipListMap<>(), EMPTY_STORAGE, tableList);
    }

    private MemStorage(NavigableMap<ByteBuffer, Record> currentStorage, NavigableMap<ByteBuffer, Record> storageToWrite, List<SSTable> tableList) {
        this.currentStorage = currentStorage;
        this.storageToWrite = storageToWrite;
        this.tableList = tableList;
    }

    public MemStorage prepareFlush() {
        return new MemStorage(new ConcurrentSkipListMap<>(), currentStorage, tableList);
    }

    public MemStorage afterFlush(SSTable ssTable) {
        List<SSTable> tableList = new ArrayList<>(this.tableList.size() + 1);
        tableList.addAll(this.tableList);
        tableList.add(ssTable);
        return new MemStorage(currentStorage, EMPTY_STORAGE, tableList);
    }

    public MemStorage afterCompaction(SSTable ssTable) {
        return new MemStorage(currentStorage, EMPTY_STORAGE, Collections.singletonList(ssTable));
    }
}
