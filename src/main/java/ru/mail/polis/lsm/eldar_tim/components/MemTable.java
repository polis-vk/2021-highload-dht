package ru.mail.polis.lsm.eldar_tim.components;

import ru.mail.polis.lsm.Record;

import java.nio.ByteBuffer;
import java.util.NavigableMap;

public interface MemTable {
    int getId();

    void put(Record record);

    NavigableMap<ByteBuffer, Record> raw();

    MemTable toReadOnly();
}
