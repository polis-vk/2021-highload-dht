package ru.mail.polis.service.igorsamokhin;

import one.nio.util.ByteArrayBuilder;
import ru.mail.polis.lsm.Record;

import java.util.Iterator;
import java.util.function.Supplier;

public class RecordSupplier implements Supplier<byte[]> {

    private final Iterator<Record> range;

    public RecordSupplier(Iterator<Record> range) {
        this.range = range;
    }

    @Override
    public byte[] get() {
        if (!range.hasNext()) {
            return null;
        }
        Record next = range.next();
        ByteArrayBuilder builder = new ByteArrayBuilder(next.size());
        builder.append(ByteBufferUtils.extractBytes(next.getKey()))
                .append("\n")
                .append(ByteBufferUtils.extractBytes(next.getValue()));
        return builder.trim();
    }
}
