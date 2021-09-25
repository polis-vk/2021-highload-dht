package ru.mail.polis.lsm;

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
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SSTable implements Closeable {

    private final MappedByteBuffer mmap;
    private final MappedByteBuffer idx;

    private static final Method CLEAN;

    static {
        try {
            Class<?> fileChannelImplClass = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = fileChannelImplClass.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    private Path filePath;
    private Path idxPath;

    public Path getFilePath() {
        return filePath;
    }

    public void setFilePath(Path dir) {
        this.filePath = dir;
    }

    public Path getIdxPath() {
        return idxPath;
    }

    public void setIdxPath(Path dir) {
        this.idxPath = dir;
    }

    static List<SSTable> loadFromDir(Path dir) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files
                    .filter(path -> path.getFileName().toString().startsWith("file_"))
                    .map(path -> path.getFileName().toString().substring(5))
                    .sorted(Comparator.comparingInt(Integer::parseInt))
                    .map(number -> getTable(dir, number))
                    .collect(Collectors.toList());
        }
    }

    private static SSTable getTable(Path dir, String number) {
        try {
            return new SSTable(dir.resolve("file_" + number), dir.resolve("idx_" + number));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Write SSTable data to hard drive.
     */
    public static SSTable write(Iterator<Record> records, Path file, Path index) throws IOException {
        Files.deleteIfExists(file);
        Files.deleteIfExists(index);

        try (
                FileChannel fileChannel = openForWrite(file);
                FileChannel indexChannel = openForWrite(index)
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

        return new SSTable(file, index);
    }

    private static FileChannel openForWrite(Path tmpFileName) throws IOException {
        return FileChannel.open(
                tmpFileName,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
        );
    }

    private static void writeValueWithSize(
            ByteBuffer value, WritableByteChannel channel, ByteBuffer tmp
    ) throws IOException {
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

    @Override
    public void close() throws IOException {
        cleanMmap(mmap);
        cleanMmap(idx);
    }

    private void cleanMmap(MappedByteBuffer mmap) throws IOException {
        try {
            CLEAN.invoke(null, mmap);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IOException(e);
        }
    }

    SSTable(Path file, Path index) throws IOException {
        this.filePath = file;
        this.idxPath = index;
        mmap = open(file);
        idx = open(index);
    }

    private static MappedByteBuffer open(Path name) throws IOException {
        try (
                FileChannel channel = FileChannel.open(name, StandardOpenOption.READ)
        ) {
            return channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        }
    }

    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey, boolean isDirectOrder) {
        synchronized (this) {
            return new DiskIterator(fromKey,
                    toKey,
                    mmap.asReadOnlyBuffer(),
                    idx.asReadOnlyBuffer(),
                    isDirectOrder);
        }
    }

}
