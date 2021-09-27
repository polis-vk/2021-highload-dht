package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

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

    @Override
    public String toString() {
        return "Record{" +
            "key=" + StandardCharsets.UTF_8.decode(key) +
            ", value=" + StandardCharsets.UTF_8.decode(value) +
            '}';
    }
}
