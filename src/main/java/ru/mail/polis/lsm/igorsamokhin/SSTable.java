package ru.mail.polis.lsm.igorsamokhin;

import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;

@SuppressWarnings("JdkObsolete")
class SSTable {
    private static final int MAX_BUFFER_SIZE = 4096;

    private final LongMappedByteBuffer mmap;
    private final MappedByteBuffer idx;

    public SSTable(Path file) throws IOException {
        Path indexFile = FileUtils.getIndexFile(file);

        idx = FileUtils.openForRead(indexFile);
        int[] sizes = getBuffersSizes(idx);
        idx.limit(idx.limit() - Integer.BYTES * (sizes.length + 1));
        mmap = FileUtils.openStorageForRead(file, sizes);
    }

    private int[] getBuffersSizes(MappedByteBuffer index) {
        int readPosition = index.limit() - Integer.BYTES;
        int n = index.getInt(readPosition);
        int[] sizes = new int[n];
        for (int i = 0; i < n; i++) {
            readPosition -= Integer.BYTES;
            sizes[i] = index.getInt(readPosition);
        }
        return sizes;
    }

    public void close() throws IOException {
        IOException exception = null;
        try {
            mmap.free();
        } catch (IOException e) {
            exception = e;
        }

        try {
            LongMappedByteBuffer.free(idx);
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
        long maxSize = mmap.remaining();

        long fromOffset = fromKey == null ? 0 : offset(mmap, fromKey);
        long toOffset = toKey == null ? maxSize : offset(mmap, toKey);

        return new ByteBufferRecordIterator(
                mmap,
                fromOffset == -1 ? maxSize : fromOffset,
                toOffset == -1 ? maxSize : toOffset
        );
    }

    public static List<SSTable> loadFromDir(Path dir) throws IOException {
        FileUtils.prepareDirectory(dir);
        File[] files = dir.toFile().listFiles();

        ArrayList<SSTable> ssTables = new ArrayList<>();
        if ((files == null) || (files.length == 0)) {
            return ssTables;
        }
        Arrays.sort(files);

        for (File file : files) {
            String name = file.getName();
            if (!name.endsWith(FileUtils.TMP_FILE_EXT) && !name.endsWith(FileUtils.INDEX_FILE_EXT)) {
                ssTables.add(new SSTable(file.toPath()));
            }
        }
        return ssTables;
    }

    public static SSTable loadFromFile(Path file) throws IOException {
        return new SSTable(file);
    }

    public static void write(Iterator<Record> records, Path file) throws IOException {
        Path indexFile = FileUtils.getIndexFile(file);
        Path tmpFileName = FileUtils.getTmpFile(file);
        Path tmpIndexName = FileUtils.getTmpFile(indexFile);

        try (FileChannel fileChannel = FileUtils.openForWrite(tmpFileName);
             FileChannel indexChannel = FileUtils.openForWrite(tmpIndexName)
        ) {
            final ByteBuffer tmp = ByteBuffer.allocate(MAX_BUFFER_SIZE);
            tmp.limit(tmp.capacity());

            List<Integer> sizes = new ArrayList<>();
            //можно попробовать писать за 1 раз не один record, а столько, сколько влезет в буфер.
            long offset = 0;
            while (records.hasNext()) {
                Record record = records.next();
                int size = record.size();
                long position = fileChannel.position();
                if (position + size + Integer.BYTES * 2 > Integer.MAX_VALUE) {
                    sizes.add((int) (position - offset));
                    offset += Integer.MAX_VALUE;
                }

                ByteBufferUtils.writeLong(position, indexChannel, tmp);

                writeRecord(fileChannel, tmp, record);
            }

            sizes.add((int) (fileChannel.position() - offset));

            //todo сделать буферизацию
            int n = sizes.size();
            for (int i = n - 1; i >= 0; --i) {
                ByteBufferUtils.writeInt(sizes.get(i), indexChannel, tmp);
            }
            ByteBufferUtils.writeInt(n, indexChannel, tmp);

            fileChannel.force(false);
            indexChannel.force(false);
        }

        FileUtils.rename(indexFile, tmpIndexName);
        FileUtils.rename(file, tmpFileName);
    }

    private static void writeRecord(FileChannel fileChannel, ByteBuffer tmp, Record record) throws IOException {
        ByteBuffer key = record.getKey();
        ByteBuffer value = record.getValue();

        ByteBufferUtils.writeBuffersWithSize(fileChannel, tmp, key, value);
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
        if ((files == null) || (files.length == 0)) {
            return null;
        }
        Arrays.sort(files);

        Path fileName = dir.resolve(FileUtils.COMPACT_FILE_NAME + files[0].getName());
        write(range, fileName);
        return fileName;
    }

    private long offset(LongMappedByteBuffer buffer, ByteBuffer keyToFind) {
        long left = 0;
        long rightLimit = idx.remaining() / Long.BYTES;
        long right = rightLimit;

        int keyToFindSize = keyToFind.remaining();

        while (left < right) {
            int mid = (int) (left + ((right - left) >>> 1));

            long offset = idx.getLong(mid * Long.BYTES);
            buffer.position(offset);
            int existingKeySize = buffer.getInt();

            int mismatchPos = buffer.mismatch(keyToFind);
            if (mismatchPos == -1) {
                return offset;
            }

            if (existingKeySize == keyToFindSize && existingKeySize == mismatchPos) {
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
                left = mid + 1L;
            } else {
                right = mid;
            }
        }

        if (left >= rightLimit) {
            return -1;
        }

        return idx.getLong((int) (left * Long.BYTES));
    }
}
