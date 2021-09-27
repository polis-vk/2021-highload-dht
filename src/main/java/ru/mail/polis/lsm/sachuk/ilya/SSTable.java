package ru.mail.polis.lsm.sachuk.ilya;

import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.sachuk.ilya.iterators.SSTableIterator;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
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
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SSTable {

    public static final String FIRST_SAVE_FILE = "SSTABLE0.save";
    public static final String FIRST_INDEX_FILE = "INDEX0.index";

    private static final String SAVE_FILE = "SSTABLE";
    private static final String SAVE_FILE_END = ".save";

    private static final String INDEX_FILE = "INDEX";
    private static final String INDEX_FILE_END = ".index";

    private static final String TMP_FILE = "TMP";
    private static final String NULL_VALUE = "NULL_VALUE";
    public static final ByteBuffer BYTE_BUFFER_TOMBSTONE = ByteBuffer.wrap(
            NULL_VALUE.getBytes(StandardCharsets.UTF_8)
    );

    private final Path savePath;
    private final Path indexPath;
    private int[] indexes;

    private MappedByteBuffer mappedByteBuffer;
    private MappedByteBuffer indexByteBuffer;

    SSTable(Path savePath, Path indexPath) throws IOException {
        this.savePath = savePath;
        this.indexPath = indexPath;

        restoreStorage();
    }

    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {

        if (fromKey != null && toKey != null && fromKey.compareTo(toKey) == 0) {
            return Collections.emptyIterator();
        }

        return new SSTableIterator(binarySearchKey(indexes, fromKey), toKey, mappedByteBuffer);
    }

    public static List<SSTable> loadFromDir(Path dir) throws IOException {

        List<SSTable> listSSTables = new ArrayList<>();

        Iterator<Path> savePaths = getPathIterator(dir, SAVE_FILE_END);
        Iterator<Path> indexPaths = getPathIterator(dir, INDEX_FILE_END);

        while (savePaths.hasNext() && indexPaths.hasNext()) {
            Path savePath = savePaths.next();
            Path indexPath = indexPaths.next();

            listSSTables.add(new SSTable(savePath, indexPath));
        }

        return listSSTables;
    }

    public static SSTable save(Iterator<Record> iterators, Path dir, int fileNumber) throws IOException {

        final Path savePath = dir.resolve(SAVE_FILE + fileNumber + SAVE_FILE_END);
        final Path indexPath = dir.resolve(INDEX_FILE + fileNumber + INDEX_FILE_END);

        Path tmpSavePath = dir.resolve(SAVE_FILE + "_" + TMP_FILE);
        Path tmpIndexPath = dir.resolve(INDEX_FILE + "_" + TMP_FILE);

        Files.deleteIfExists(tmpSavePath);
        Files.deleteIfExists(tmpIndexPath);

        try (FileChannel saveFileChannel = openFileChannel(tmpSavePath)) {
            try (FileChannel indexFileChanel = openFileChannel(tmpIndexPath)) {

                ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);

                int counter = 0;
                writeValue(indexFileChanel, counter);

                while (iterators.hasNext()) {
                    int indexPositionToRead = (int) saveFileChannel.position();

                    writeValue(indexFileChanel, indexPositionToRead);
                    counter++;

                    Record record = iterators.next();

                    ByteBuffer value = record.getValue() == null
                            ? ByteBuffer.wrap(NULL_VALUE.getBytes(StandardCharsets.UTF_8))
                            : record.getValue();

                    writeSizeAndValue(record.getKey(), saveFileChannel, size);
                    writeSizeAndValue(value, saveFileChannel, size);
                }

                int curPos = (int) indexFileChanel.position();

                indexFileChanel.position(0);

                writeValue(indexFileChanel, counter);

                indexFileChanel.position(curPos);

                saveFileChannel.force(false);
                indexFileChanel.force(false);
            }
        }

        Files.deleteIfExists(savePath);
        Files.deleteIfExists(indexPath);

        Files.move(tmpSavePath, savePath, StandardCopyOption.ATOMIC_MOVE);
        Files.move(tmpIndexPath, indexPath, StandardCopyOption.ATOMIC_MOVE);

        return new SSTable(savePath, indexPath);
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

    public Path getSavePath() {
        return savePath;
    }

    public Path getIndexPath() {
        return indexPath;
    }

    private int binarySearchKey(int[] indexArray, ByteBuffer keyToFind) {

        if (keyToFind == null) {
            return 0;
        }

        int start = 0;
        int end = indexArray.length - 1;

        int positionToRead;

        int middle = (start + end) / 2;

        while (start <= end) {

            middle = (start + end) / 2;

            positionToRead = indexArray[middle];

            mappedByteBuffer.position(positionToRead);

            ByteBuffer key = readFromFile(mappedByteBuffer);

            if (keyToFind.compareTo(key) == 0) {
                return positionToRead;
            } else if (keyToFind.compareTo(key) > 0) {
                start = middle + 1;

                if (start > end && keyToFind.compareTo(key) > 0) {

                    if (start == indexArray.length) {
                        return -1;
                    }

                    if (start < indexArray.length) {
                        return start;
                    }
                }
            } else {
                end = middle - 1;
            }

        }
        return indexArray[middle];
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

    public static ByteBuffer readFromFile(MappedByteBuffer mappedByteBuffer) {
        int length = mappedByteBuffer.getInt();

        ByteBuffer byteBuffer = mappedByteBuffer.slice().limit(length).asReadOnlyBuffer();
        mappedByteBuffer.position(mappedByteBuffer.position() + length);

        return byteBuffer;
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

    private static void writeValue(FileChannel fileChannel, int value) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.wrap(
                ByteBuffer.allocate(Integer.BYTES).putInt(value).array()
        );

        fileChannel.write(byteBuffer);
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

    private static Iterator<Path> getPathIterator(Path dir, String pathEnd) throws IOException {
        Iterator<Path> paths;
        try (Stream<Path> streamPaths = Files.walk(Paths.get(dir.toUri()))) {
            paths = streamPaths.filter(path -> path.toString().endsWith(pathEnd))
                    .collect(Collectors.toList())
                    .stream()
                    .sorted(Comparator.comparing(o -> getFileNumber(o, pathEnd)))
                    .collect(Collectors.toList())
                    .iterator();
        }
        return paths;
    }

    private static Integer getFileNumber(Path path, String endFile) {
        String stringPath = path.toString();

        int lastSlash = 0;

        for (int i = 0; i < stringPath.length(); i++) {
            if (stringPath.charAt(i) == File.separatorChar) {
                lastSlash = i;
            }
        }

        stringPath = stringPath.substring(lastSlash + 1);

        int firstNumberIndex = 0;

        for (int i = 0; i < stringPath.length(); i++) {
            if (Character.isDigit(stringPath.charAt(i))) {
                firstNumberIndex = i;
                break;
            }
        }

        return Integer.parseInt(stringPath.substring(firstNumberIndex, stringPath.length() - endFile.length()));
    }

    private static FileChannel openFileChannel(Path path) throws IOException {
        return FileChannel.open(
                path,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
    }
}
