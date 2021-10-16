package ru.mail.polis.lsm.eldar_tim.components;

import ru.mail.polis.lsm.Record;

import java.nio.ByteBuffer;

public interface MemTable {
    Record put(ByteBuffer key, Record value);

    MemTable toReadOnly();
}