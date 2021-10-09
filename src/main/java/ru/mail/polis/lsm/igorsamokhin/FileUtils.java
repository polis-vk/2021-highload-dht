package ru.mail.polis.lsm.igorsamokhin;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

final class FileUtils {
    public static final String TMP_FILE_EXT = ".tmp";
    public static final String INDEX_FILE_EXT = ".idx";
    public static final String COMPACT_FILE_NAME = "COMPACT_";

    private FileUtils() {
    }

    public static MappedByteBuffer openForRead(Path name) throws IOException {
        try (FileChannel channel = FileChannel.open(name, StandardOpenOption.READ)) {
            return channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        }
    }

    public static LongMappedByteBuffer openStorageForRead(Path name, int... bufferSizes) throws IOException {
        try (FileChannel channel = FileChannel.open(name, StandardOpenOption.READ)) {

            int length = bufferSizes.length;
            ByteBuffer[] buffers = new ByteBuffer[length];

            long min = 0;
            long max = 0;
            for (long i = 0; i < length; i++) {
                min = max + i * Integer.MAX_VALUE;
                max = min + bufferSizes[(int) i];

                buffers[(int) i] = channel.map(FileChannel.MapMode.READ_ONLY, min, max);
            }
            return new LongMappedByteBuffer(buffers);
        }
    }

    public static FileChannel openForWrite(Path tmpFileName) throws IOException {
        return FileChannel.open(
                tmpFileName,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
    }

    public static Path resolveWithExt(Path file, String ext) {
        return file.resolveSibling(file.getFileName() + ext);
    }

    public static Path getIndexFile(Path file) {
        return resolveWithExt(file, INDEX_FILE_EXT);
    }

    public static Path getTmpFile(Path file) {
        return resolveWithExt(file, TMP_FILE_EXT);
    }

    public static void rename(Path file, Path tmpFile) throws IOException {
        Files.deleteIfExists(file);
        Files.move(tmpFile, file, StandardCopyOption.ATOMIC_MOVE);
    }

    /**
     * Delete all files if there is compact file inside dir.
     *
     * @param dir directory
     * @return true if compact files deleted
     */
    public static boolean prepareDirectory(Path dir) throws IOException {
        File[] files = dir.toFile().listFiles();

        Arrays.sort(files);

        if ((files.length == 0) || !files[0].getName().startsWith(COMPACT_FILE_NAME)) {
            return false;
        }

        for (File file : files) {
            if (!file.getName().startsWith(COMPACT_FILE_NAME)) {
                Files.delete(file.toPath());
            }
        }

        Path compactFile = files[0].toPath();
        String fileName = files[0].getName().substring(COMPACT_FILE_NAME.length());
        Path file = dir.resolve(fileName);

        if (Files.exists(FileUtils.getIndexFile(compactFile))) {
            Files.move(FileUtils.getIndexFile(compactFile),
                    FileUtils.getIndexFile(file),
                    StandardCopyOption.ATOMIC_MOVE);
        }

        Files.move(compactFile, file, StandardCopyOption.ATOMIC_MOVE);
        return true;
    }
}
