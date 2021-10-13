package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class TestDaoWrapper implements DAO {
    static final long MAX_HEAP = 128 * 1024 * 1024;

    private final DAO delegate;

    private TestDaoWrapper(DAO delegate) {
        this.delegate = delegate;
    }

    public static DAO create(DAOConfig config) throws IOException {
        if (Runtime.getRuntime().maxMemory() > MAX_HEAP) {
            throw new IllegalStateException("The heap is too big. Consider setting Xmx.");
        }
        return new TestDaoWrapper(DAOFactory.create(config));
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return delegate.range(toReadOnly(fromKey), toReadOnly(toKey));
    }

    @Override
    public void upsert(Record record) {
        delegate.upsert(record);
    }

    @Override
    public void compact() {
        delegate.compact();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    private ByteBuffer toReadOnly(@Nullable ByteBuffer key) {
        return key == null ? null : key.asReadOnlyBuffer();
    }

}
