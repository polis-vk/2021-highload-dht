package ru.mail.polis.lsm;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;

@SuppressWarnings("JavaLangClash")
public class Record {

    private final ByteBuffer key;
    private final ByteBuffer value;

    Record(ByteBuffer key, @Nullable ByteBuffer value) {
        this.key = key.asReadOnlyBuffer();
        this.value = value == null ? null : value.asReadOnlyBuffer();
    }

    public static Record of(ByteBuffer key, ByteBuffer value) {
        return new Record(key.asReadOnlyBuffer(), value.asReadOnlyBuffer());
    }

    public static Record tombstone(ByteBuffer key) {
        return new Record(key, null);
    }

    public ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    public ByteBuffer getValue() {
        return value == null ? null : value.asReadOnlyBuffer();
    }

    public boolean isTombstone() {
        return value == null;
    }

    public int getKeySize() {
        return key.remaining();
    }

    public int getValueSize() {
        return value == null ? 0 : value.remaining();
    }

    public int compareKeyWith(Record r) {
        return this.key.compareTo(r.key); //много памяти используется для того, чтобы достать ключи и сравнить их
    }

    public int compareValueWith(Record r) {
        return this.value.compareTo(r.value); //много памяти используется для того, чтобы достать ключи и сравнить их
    }

    public int size() {
        return getKeySize() + getValueSize();
    }
}
