package ru.mail.polis.lsm.artem_drozdov;

import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Stream;

public class SSTable implements Closeable {

    public static final String SSTABLE_FILE_PREFIX = "file_";
    public static final String COMPACTION_FILE_NAME = "compaction";

    private static final Method CLEAN;

    static {
        try {
            Class<?> aClass = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = aClass.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private final MappedByteBuffer mmap;
    private final MappedByteBuffer idx;

    public static List<SSTable> loadFromDir(Path dir) throws IOException {
        Path compaction = dir.resolve(COMPACTION_FILE_NAME);
        if (Files.exists(compaction)) {
            finishCompaction(dir);
        }
        List<SSTable> result = new ArrayList<>();
        for (int i = 0; ; i++) {
            Path file = dir.resolve(SSTABLE_FILE_PREFIX + i);
            if (!Files.exists(file)) {
                return result;
            }
            result.add(new SSTable(file));
        }
    }

    public static SSTable write(Iterator<Record> records, Path file) throws IOException {
        writeImpl(records, file);

        return new SSTable(file);
    }

    private static void writeImpl(Iterator<Record> records, Path file) throws IOException {
        Path indexFile = getIndexFile(file);
        Path tmpFileName = getTmpFile(file);
        Path tmpIndexName = getTmpFile(indexFile);

        try (
                FileChannel fileChannel = openForWrite(tmpFileName);
                FileChannel indexChannel = openForWrite(tmpIndexName)
        ) {
            ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            while (records.hasNext()) {
                long position = fileChannel.position();
                if (position > Integer.MAX_VALUE) {
                    throw new IllegalStateException("File is too long");
                }
                writeInt((int) position, indexChannel, size);

                Record record = records.next();
                writeValueWithSize(record.getKey(), fileChannel, size);
                if (record.isTombstone()) {
                    writeInt(-1, fileChannel, size);
                } else {
                    // value is null for tombstones only
                    ByteBuffer value = Objects.requireNonNull(record.getValue());
                    writeValueWithSize(value, fileChannel, size);
                }
            }
            fileChannel.force(false);
        }

        rename(indexFile, tmpIndexName);
        rename(file, tmpFileName);
    }

    public static SSTable compact(Path dir, Iterator<Record> records) throws IOException {
        Path compaction = dir.resolve("compaction");
        writeImpl(records, compaction);

        for (int i = 0; ; i++) {
            Path file = dir.resolve(SSTABLE_FILE_PREFIX + i);
            if (!Files.deleteIfExists(file)) {
                break;
            }
            Files.deleteIfExists(getIndexFile(file));
        }

        Path file0 = dir.resolve(SSTABLE_FILE_PREFIX + 0);
        if (Files.exists(getIndexFile(compaction))) {
            Files.move(getIndexFile(compaction), getIndexFile(file0), StandardCopyOption.ATOMIC_MOVE);
        }
        Files.move(compaction, file0, StandardCopyOption.ATOMIC_MOVE);
        return new SSTable(file0);
    }

    private static void finishCompaction(Path dir) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            files.filter(file -> file.getFileName().startsWith(SSTABLE_FILE_PREFIX))
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        }

        Path compaction = dir.resolve(COMPACTION_FILE_NAME);

        Path file0 = dir.resolve(SSTABLE_FILE_PREFIX + 0);
        if (Files.exists(getIndexFile(compaction))) {
            Files.move(getIndexFile(compaction), getIndexFile(file0), StandardCopyOption.ATOMIC_MOVE);
        }

        Files.move(compaction, file0, StandardCopyOption.ATOMIC_MOVE);
    }

    public SSTable(Path file) throws IOException {
        Path indexFile = getIndexFile(file);

        mmap = open(file);
        idx = open(indexFile);
    }

    public static int sizeOf(Record record) {
        int keySize = Integer.BYTES + record.getKeySize();
        int valueSize = Integer.BYTES + record.getValueSize();
        return keySize + valueSize;
    }

    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        ByteBuffer buffer = mmap.asReadOnlyBuffer();

        int maxSize = mmap.remaining();

        int fromOffset = fromKey == null ? 0 : offset(buffer, fromKey);
        int toOffset = toKey == null ? maxSize : offset(buffer, toKey);

        return range(
                buffer,
                fromOffset == -1 ? maxSize : fromOffset,
                toOffset == -1 ? maxSize : toOffset
        );
    }

    @Override
    public void close() throws IOException {
        IOException exception = null;
        try {
            free(mmap);
        } catch (Throwable t) {
            exception = new IOException(t);
        }

        try {
            free(idx);
        } catch (Throwable t) {
            if (exception == null) {
                exception = new IOException(t);
            } else {
                exception.addSuppressed(t);
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    private static Path resolveWithExt(Path file, String ext) {
        return file.resolveSibling(file.getFileName() + ext);
    }

    private static Path getIndexFile(Path file) {
        return resolveWithExt(file, ".idx");
    }

    private static Path getTmpFile(Path file) {
        return resolveWithExt(file, ".tmp");
    }

    private static FileChannel openForWrite(Path tmpFileName) throws IOException {
        return FileChannel.open(
                tmpFileName,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
        );
    }

    private static void writeValueWithSize(ByteBuffer value, WritableByteChannel channel, ByteBuffer tmp) throws IOException {
        writeInt(value.remaining(), channel, tmp);
        channel.write(tmp);
        channel.write(value);
    }

    private static void writeInt(int value, WritableByteChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        tmp.putInt(value);
        tmp.position(0);

        channel.write(tmp);
    }

    private static void rename(Path file, Path tmpFile) throws IOException {
        Files.deleteIfExists(file);
        Files.move(tmpFile, file, StandardCopyOption.ATOMIC_MOVE);
    }

    private static MappedByteBuffer open(Path name) throws IOException {
        try (
                FileChannel channel = FileChannel.open(name, StandardOpenOption.READ)
        ) {
            return channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        }
    }

    private static void free(MappedByteBuffer buffer) throws IOException {
        try {
            CLEAN.invoke(null, buffer);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IOException(e);
        }
    }

    private int offset(ByteBuffer buffer, ByteBuffer keyToFind) {
        int left = 0;
        int rightLimit = idx.remaining() / Integer.BYTES;
        int right = rightLimit;

        int keyToFindSize = keyToFind.remaining();

        while (left < right) {
            int mid = left + ((right - left) >>> 1);

            int offset = idx.getInt(mid * Integer.BYTES);
            buffer.position(offset);
            int existingKeySize = buffer.getInt();

            int result;
            int mismatchPos = buffer.mismatch(keyToFind);
            if (mismatchPos == -1) {
                return offset;
            }

            if (existingKeySize == keyToFindSize && mismatchPos == existingKeySize) {
                return offset;
            }

            if (mismatchPos < existingKeySize && mismatchPos < keyToFindSize) {
                result = Byte.compare(
                        keyToFind.get(keyToFind.position() + mismatchPos),
                        buffer.get(buffer.position() + mismatchPos)
                );
            } else if (mismatchPos >= existingKeySize) {
                result = 1;
            } else {
                result = -1;
            }

            if (result > 0) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        if (left >= rightLimit) {
            return -1;
        }

        return idx.getInt(left * Integer.BYTES);
    }

    private static Iterator<Record> range(ByteBuffer buffer, int fromOffset, int toOffset) {
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
}
