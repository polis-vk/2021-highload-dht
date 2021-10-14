package ru.mail.polis.lsm.artemdrozdov;

import ru.mail.polis.lsm.Record;

import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;

public final class SmartStorage {
    final NavigableMap<ByteBuffer, Record> memory;
    final NavigableMap<ByteBuffer, Record> memorySnapshot;
    final ConcurrentLinkedDeque<SSTable> tables;

    private SmartStorage() {
        memory = getEmptyMap();
        memorySnapshot = getEmptyMap();
        tables = new ConcurrentLinkedDeque<>();
    }

    private SmartStorage(
            NavigableMap<ByteBuffer, Record> memory,
            NavigableMap<ByteBuffer, Record> memorySnapshot,
            ConcurrentLinkedDeque<SSTable> tables
    ) {
        this.memory = memory;
        this.memorySnapshot = memorySnapshot;
        this.tables = tables;
    }

    private static NavigableMap<ByteBuffer, Record> getEmptyMap() {
        return new ConcurrentSkipListMap<>();
    }

    public static SmartStorage createEmpty() {
        return new SmartStorage();
    }

    public static SmartStorage createFromTables(ConcurrentLinkedDeque<SSTable> tables) {
        return new SmartStorage(getEmptyMap(), getEmptyMap(), tables);
    }

    public static SmartStorage fromSnapshot(
            NavigableMap<ByteBuffer,
                    Record> memorySnapshot,
            ConcurrentLinkedDeque<SSTable> tables) {
        return new SmartStorage(getEmptyMap(), memorySnapshot, tables);
    }

}
