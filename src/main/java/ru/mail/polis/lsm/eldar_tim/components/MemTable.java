package ru.mail.polis.lsm.eldar_tim.components;

import ru.mail.polis.lsm.Record;

import java.nio.ByteBuffer;
import java.util.Collection;

public interface MemTable {
    int getId();

    Record put(ByteBuffer key, Record value);

    Collection<Record> values();

    MemTable toReadOnly();
}