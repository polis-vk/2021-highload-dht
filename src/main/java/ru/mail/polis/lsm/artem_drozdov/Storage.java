package ru.mail.polis.lsm.artem_drozdov;

import ru.mail.polis.lsm.Record;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public final class Storage {
    final NavigableMap<ByteBuffer, Record> currentStorage;
    final NavigableMap<ByteBuffer, Record> tmpStorage;
    final List<SSTable> tables;

    private Storage(NavigableMap<ByteBuffer,Record> currentStorage,NavigableMap<ByteBuffer,Record> tmpStorage,
                                    List<SSTable> ssTableList) {
        this.currentStorage = currentStorage;
        this.tmpStorage = tmpStorage;
        this.tables = ssTableList;
    }

    public static Storage init(List<SSTable> tables) {
        return new Storage(new ConcurrentSkipListMap<>(),Collections.emptyNavigableMap(),tables);
    }

    public Storage prepareFlush() {
        return new Storage(new ConcurrentSkipListMap<>(),currentStorage,tables);
    }

    public Storage afterFlush(SSTable ssNewTable) {
        List<SSTable> ssNewTables = new ArrayList<>(tables);
        ssNewTables.add(ssNewTable);
        return new Storage(currentStorage,Collections.emptyNavigableMap(),ssNewTables);
    }

    public Storage afterCompaction(SSTable table) {
        List<SSTable> newTables = Collections.singletonList(table);
        return new Storage(currentStorage,Collections.emptyNavigableMap(),newTables);
    }

}
