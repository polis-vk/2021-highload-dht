package ru.mail.polis.lsm.igorsamokhin;

import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;

@SuppressWarnings("JdkObsolete")
class SSTable {
    private static final Method CLEAN;
    private static final String TMP_FILE_EXT = ".tmp";
    private static final String INDEX_FILE_EXT = ".idx";
    private static final String COMPACT_FILE_NAME = "COMPACT_";

    private final MappedByteBuffer mmap;
    private final MappedByteBuffer idx;

    static {
        try {
            Class<?> name = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = name.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    public SSTable(Path file) throws IOException {
        Path indexFile = getIndexFile(file);

        mmap = openForRead(file);
        idx = openForRead(indexFile);
    }

    private static MappedByteBuffer openForRead(Path name) throws IOException {
        try (FileChannel channel = FileChannel.open(name, StandardOpenOption.READ)) {
            return channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        }
    }

    private static Path resolveWithExt(Path file, String ext) {
        return file.resolveSibling(file.getFileName() + ext);
    }

    private static Path getIndexFile(Path file) {
        return resolveWithExt(file, INDEX_FILE_EXT);
    }

    private static Path getTmpFile(Path file) {
        return resolveWithExt(file, TMP_FILE_EXT);
    }

    private static FileChannel openForWrite(Path tmpFileName) throws IOException {
        return FileChannel.open(
                tmpFileName,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
    }

    private static void free(MappedByteBuffer buffer) throws IOException {
        try {
            CLEAN.invoke(null, buffer);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IOException(e);
        }
    }

    public void close() throws IOException {
        IOException exception = null;
        try {
            free(mmap);
        } catch (IOException e) {
            exception = e;
        }

        try {
            free(idx);
        } catch (IOException e) {
            if (exception == null) {
                exception = e;
            } else {
                exception.addSuppressed(e);
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        synchronized (this) {
            ByteBuffer buffer = mmap.asReadOnlyBuffer();

            int maxSize = mmap.remaining();

            int fromOffset = fromKey == null ? 0 : offset(buffer, fromKey);
            int toOffset = toKey == null ? maxSize : offset(buffer, toKey);

            return new ByteBufferRecordIterator(
                    buffer,
                    fromOffset == -1 ? maxSize : fromOffset,
                    toOffset == -1 ? maxSize : toOffset
            );
        }
    }

    public static List<SSTable> loadFromDir(Path dir) throws IOException {
        prepareDirectory(dir);
        File[] files = dir.toFile().listFiles();
        ArrayList<SSTable> ssTables = new ArrayList<>();
        if (files.length == 0) {
            return ssTables;
        }

        for (File file : files) {
            String name = file.getName();
            if (!name.endsWith(TMP_FILE_EXT) && !name.endsWith(INDEX_FILE_EXT)) {
                ssTables.add(new SSTable(file.toPath()));
            }
        }
        return ssTables;
    }

    public static SSTable loadFromFile(Path file) throws IOException {
        return new SSTable(file);
    }

    public static void write(Iterator<Record> records, Path file) throws IOException {
        Path indexFile = getIndexFile(file);
        Path tmpFileName = getTmpFile(file);
        Path tmpIndexName = getTmpFile(indexFile);

        try (FileChannel fileChannel = openForWrite(tmpFileName);
             FileChannel indexChannel = openForWrite(tmpIndexName)
        ) {
            final ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            while (records.hasNext()) {
                long position = fileChannel.position();
                if (position > Integer.MAX_VALUE) {
                    throw new IllegalStateException("File is too long");
                }
                ByteBufferUtils.writeInt((int) position, indexChannel, size);

                Record record = records.next();
                ByteBufferUtils.writeValue(record.getKey(), fileChannel, size);
                ByteBufferUtils.writeValue(record.getValue(), fileChannel, size);
            }
            fileChannel.force(false);
        }

        rename(indexFile, tmpIndexName);
        rename(file, tmpFileName);
    }

    private static void rename(Path file, Path tmpFile) throws IOException {
        Files.deleteIfExists(file);
        Files.move(tmpFile, file, StandardCopyOption.ATOMIC_MOVE);
    }

    /**
     * Create sub map.
     */
    public static SortedMap<ByteBuffer, Record> getSubMap(SortedMap<ByteBuffer, Record> memoryStorage,
                                                   @Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return memoryStorage;
        } else if (fromKey == null) {
            return memoryStorage.headMap(toKey);
        } else if (toKey == null) {
            return memoryStorage.tailMap(fromKey);
        }
        return memoryStorage.subMap(fromKey, toKey);
    }

    /**
     * Compact several sstables in dir into one.
     */
    public static Path compact(Path dir, Iterator<Record> range) throws IOException {
        File[] files = dir.toFile().listFiles();
        if (files.length == 0) {
            return null;
        }

        Path fileName = dir.resolve(COMPACT_FILE_NAME + files[0].getName());
        write(range, fileName);
        return fileName;
    }

    /**
     * Delete all files if there is compact file inside dir.
     *
     * @param dir directory
     * @return true if compact files deleted
     */
    public static boolean prepareDirectory(Path dir) throws IOException {
        File[] files = dir.toFile().listFiles();

        if ((files.length == 0) || !files[0].getName().startsWith(COMPACT_FILE_NAME)) {
            return false;
        }

        for (File file : files) {
            if (!file.getName().startsWith(COMPACT_FILE_NAME)) {
                file.delete();
            }
        }

        Path compactFile = files[0].toPath();
        String fileName = files[0].getName().substring(COMPACT_FILE_NAME.length());
        Path file = dir.resolve(fileName);

        if (Files.exists(getIndexFile(compactFile))) {
            Files.move(getIndexFile(compactFile), getIndexFile(file), StandardCopyOption.ATOMIC_MOVE);
        }

        Files.move(compactFile, file, StandardCopyOption.ATOMIC_MOVE);
        return true;
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

        return idx.getInt(left * Integer.BYTES);
    }
}
