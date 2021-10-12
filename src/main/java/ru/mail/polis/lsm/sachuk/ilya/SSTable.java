package ru.mail.polis.lsm.sachuk.ilya;

import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.sachuk.ilya.iterators.ByteBufferRecordIterator;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class SSTable {
    public static final String COMPACTION_FILE_NAME = "compaction";
    public static final String SAVE_FILE_PREFIX = "SSTABLE_";
    private static final String INDEX_FILE_END = ".index";
    private static final String TMP_FILE_END = ".tmp";

    private static final String NULL_VALUE = "NULL_VALUE";
    public static final ByteBuffer BYTE_BUFFER_TOMBSTONE = ByteBuffer.wrap(
            NULL_VALUE.getBytes(StandardCharsets.UTF_8)
    );

    private final Path savePath;
    private final Path indexPath;
    private int[] indexes;
    private MappedByteBuffer mappedByteBuffer;
    private MappedByteBuffer indexByteBuffer;

    SSTable(Path savePath) throws IOException {
        this.savePath = savePath;
        this.indexPath = getIndexFile(savePath);

        restoreStorage();
    }

    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        ByteBuffer buffer = mappedByteBuffer.asReadOnlyBuffer();

        int maxSize = mappedByteBuffer.remaining();

        int fromOffset = fromKey == null ? 0 : offset(buffer, fromKey);
        int toOffset = toKey == null ? maxSize : offset(buffer, toKey);

        return new ByteBufferRecordIterator(
                buffer,
                fromOffset == -1 ? maxSize : fromOffset,
                toOffset == -1 ? maxSize : toOffset
        );
    }

    private int offset(ByteBuffer buffer, ByteBuffer keyToFind) {
        indexByteBuffer.position(0);
        int left = 0;
        int rightLimit = indexByteBuffer.remaining() / Integer.BYTES;
        int right = rightLimit;

        int keyToFindSize = keyToFind.remaining();

        while (left < right) {
            int mid = left + ((right - left) >>> 1);

            int offset = indexByteBuffer.getInt(mid * Integer.BYTES);
            buffer.position(offset);
            int existingKeySize = buffer.getInt();

            int mismatchPos = buffer.mismatch(keyToFind);
            if (mismatchPos == -1) {
                return offset;
            }

            if (existingKeySize == keyToFindSize && mismatchPos == existingKeySize) {
                return offset;
            }

            final int result;
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

        indexByteBuffer.position(0);
        return indexByteBuffer.getInt(left * Integer.BYTES);
    }

    public static List<SSTable> loadFromDir(Path dir) throws IOException {
        Path compaction = dir.resolve(COMPACTION_FILE_NAME);
        if (Files.exists(compaction)) {
            finishCompaction(dir);
        }

        List<SSTable> result = new ArrayList<>();
        for (int i = 0; ; i++) {
            Path file = dir.resolve(SAVE_FILE_PREFIX + i);
            if (!Files.exists(file)) {
                return result;
            }
            result.add(new SSTable(file));
        }
    }

    public static SSTable save(Iterator<Record> iterators, Path savePath) throws IOException {

        Path indexPath = getIndexFile(savePath);
        Path tmpSavePath = getTmpFile(savePath);
        Path tmpIndexPath = getTmpFile(indexPath);

        Files.deleteIfExists(tmpSavePath);
        Files.deleteIfExists(tmpIndexPath);

        try (FileChannel saveFileChannel = openFileChannel(tmpSavePath)) {
            try (FileChannel indexFileChanel = openFileChannel(tmpIndexPath)) {

                ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);

                int counter = 0;
                writeInt(indexFileChanel, buffer, counter);

                while (iterators.hasNext()) {
                    long longPosition = saveFileChannel.position();
                    if (longPosition > Integer.MAX_VALUE) {
                        throw new IllegalStateException("File is too long");
                    }

                    int indexPositionToRead = (int) longPosition;

                    writeInt(indexFileChanel, buffer, indexPositionToRead);
                    counter++;

                    Record record = iterators.next();
                    ByteBuffer value = record.isTombstone()
                            ? BYTE_BUFFER_TOMBSTONE
                            : record.getValue();

                    writeSizeAndValue(record.getKey(), saveFileChannel, buffer);

                    if (record.isTombstone()) {
                        writeInt(saveFileChannel, buffer, -1);
                    } else {
                        writeSizeAndValue(value, saveFileChannel, buffer);
                    }
                }

                int curPos = (int) indexFileChanel.position();

                indexFileChanel.position(0);

                writeInt(indexFileChanel, buffer, counter);

                indexFileChanel.position(curPos);

                saveFileChannel.force(false);
                indexFileChanel.force(false);
            }
        }

        Files.deleteIfExists(savePath);
        Files.deleteIfExists(indexPath);

        Files.move(tmpSavePath, savePath, StandardCopyOption.ATOMIC_MOVE);
        Files.move(tmpIndexPath, indexPath, StandardCopyOption.ATOMIC_MOVE);

        return new SSTable(savePath);
    }

    public static SSTable compact(Path dir, Iterator<Record> records) throws IOException {
        Path compaction = dir.resolve("compaction");
        save(records, compaction);

        for (int i = 0; ; i++) {
            Path file = dir.resolve(SAVE_FILE_PREFIX + i);
            if (!Files.deleteIfExists(file)) {
                break;
            }
            Files.deleteIfExists(getIndexFile(file));
        }

        Path file0 = dir.resolve(SAVE_FILE_PREFIX + 0);
        if (Files.exists(getIndexFile(compaction))) {
            Files.move(getIndexFile(compaction), getIndexFile(file0), StandardCopyOption.ATOMIC_MOVE);
        }
        Files.move(compaction, file0, StandardCopyOption.ATOMIC_MOVE);
        return new SSTable(file0);
    }

    private static void finishCompaction(Path dir) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            files.filter(file -> file.getFileName().startsWith(SAVE_FILE_PREFIX))
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        }

        Path compaction = dir.resolve(COMPACTION_FILE_NAME);

        Path file0 = dir.resolve(SAVE_FILE_PREFIX + 0);
        if (Files.exists(getIndexFile(compaction))) {
            Files.move(getIndexFile(compaction), getIndexFile(file0), StandardCopyOption.ATOMIC_MOVE);
        }

        Files.move(compaction, file0, StandardCopyOption.ATOMIC_MOVE);
    }

    private static Path resolveWithExt(Path file, String ext) {
        return file.resolveSibling(file.getFileName() + ext);
    }

    private static Path getIndexFile(Path file) {
        return resolveWithExt(file, INDEX_FILE_END);
    }

    private static Path getTmpFile(Path file) {
        return resolveWithExt(file, TMP_FILE_END);
    }

    public void close() throws IOException {
        if (mappedByteBuffer != null) {
            clean(mappedByteBuffer);
        }

        if (indexByteBuffer != null) {
            clean(indexByteBuffer);
            indexByteBuffer.clear();
            Arrays.fill(indexes, 0);
            indexes = null;
        }
    }

    private void restoreStorage() throws IOException {
        try (FileChannel saveFileChannel = FileChannel.open(savePath, StandardOpenOption.READ)) {
            try (FileChannel indexFileChannel = FileChannel.open(indexPath, StandardOpenOption.READ)) {

                mappedByteBuffer = saveFileChannel.map(
                        FileChannel.MapMode.READ_ONLY,
                        0,
                        saveFileChannel.size()
                );
                indexByteBuffer = indexFileChannel.map(
                        FileChannel.MapMode.READ_ONLY,
                        0,
                        indexFileChannel.size()
                );

                int size = indexByteBuffer.getInt();
                indexes = new int[size];

                int counter = 0;
                while (indexByteBuffer.hasRemaining()) {

                    int value = indexByteBuffer.getInt();

                    indexes[counter] = value;
                    counter++;
                }
            }
        }
    }

    private static void writeSizeAndValue(
            ByteBuffer value,
            WritableByteChannel channel,
            ByteBuffer tmp
    ) throws IOException {
        tmp.position(0);
        tmp.putInt(value.remaining());
        tmp.position(0);

        channel.write(tmp);
        channel.write(value);
    }

    private static void writeInt(WritableByteChannel fileChannel, ByteBuffer tmp, int value) throws IOException {
        tmp.position(0);
        tmp.putInt(value);
        tmp.position(0);

        fileChannel.write(tmp);
    }

    private void clean(MappedByteBuffer mappedByteBuffer) throws IOException {
        try {
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            Object unsafe = unsafeField.get(null);
            Method invokeCleaner = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
            invokeCleaner.invoke(unsafe, mappedByteBuffer);
        } catch (ClassNotFoundException | NoSuchFieldException | NoSuchMethodException
                | IllegalAccessException | InvocationTargetException e) {
            throw new IOException(e);
        }
    }

    private static FileChannel openFileChannel(Path path) throws IOException {
        return FileChannel.open(
                path,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
    }
}
