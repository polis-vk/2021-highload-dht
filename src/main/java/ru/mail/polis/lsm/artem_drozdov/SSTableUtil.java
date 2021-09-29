package ru.mail.polis.lsm.artem_drozdov;

import ru.mail.polis.lsm.Record;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;

public final class SSTableUtil {

    private SSTableUtil() {
    }

    public static Iterator<Record> range(ByteBuffer buffer, int fromOffset, int toOffset) {
        buffer.position(fromOffset);

        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return buffer.position() < toOffset;
            }

            @Override
            public Record next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("Limit is reached");
                }

                int keySize = buffer.getInt();
                ByteBuffer key = read(keySize);

                int valueSize = buffer.getInt();
                if (valueSize == -1) {
                    return Record.tombstone(key);
                }
                ByteBuffer value = read(valueSize);

                return Record.of(key, value);
            }

            private ByteBuffer read(int size) {
                ByteBuffer result = buffer.slice().limit(size);
                buffer.position(buffer.position() + size);
                return result;
            }
        };
    }

    public static int sizeOf(Record record) {
        int keySize = Integer.BYTES + record.getKeySize();
        int valueSize = Integer.BYTES + record.getValueSize();
        return keySize + valueSize;
    }

    public static Path resolveWithExt(Path file, String ext) {
        return file.resolveSibling(file.getFileName() + ext);
    }

    public static Path getIndexFile(Path file) {
        return resolveWithExt(file, ".idx");
    }

}
