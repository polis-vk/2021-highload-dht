package ru.mail.polis.lsm.igorsamokhin;

import javax.annotation.Nullable;

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
    public static final String COMPACT_FILE_EXT = ".cmp";
    public static final int FILE_NAME_LENGTH = 64;

    private FileUtils() {
    }

    public static String formatFileName(@Nullable String filePrefix, int number, @Nullable String suffix) {
        String binary = Long.toBinaryString(number);
        int leadingN = FILE_NAME_LENGTH - binary.length();

        String zeros = "0".repeat(leadingN);
        StringBuilder result = new StringBuilder();
        if (filePrefix != null) {
            result.append(filePrefix);
        }
        result.append(zeros)
                .append(binary);
        if (suffix != null) {
            result.append(suffix);
        }
        return result.toString();
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

            long begin = 0;
            long size = 0;
            for (long i = 0; i < length; i++) {
                begin += size;
                size = bufferSizes[(int) i];

                buffers[(int) i] = channel.map(FileChannel.MapMode.READ_ONLY, begin, size).asReadOnlyBuffer();
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

        if (files == null) {
            return false;
        }

        Arrays.sort(files, (a, b) -> {
            int compareTo = a.compareTo(b);
            return compareTo * -1;
        });

        boolean wasCompact = removeFilesIfWasCompaction(files);
        shiftFileNamesToBeginning(dir, files);

        return wasCompact;
    }

    /**
     * Return true if was compaction, otherwise - false.
     */
    public static boolean removeFilesIfWasCompaction(File... files) throws IOException {
        boolean wasCompact = false;
        for (int i = 0; i < files.length; i++) {
            File file = files[i];
            if (file == null) {
                continue;
            }

            if (wasCompact) {
                Files.delete(file.toPath());
                files[i] = null;
            } else if (file.getName().endsWith(COMPACT_FILE_EXT)) {
                wasCompact = true;
            }
        }
        return wasCompact;
    }

    private static void shiftFileNamesToBeginning(Path dir, File... files) throws IOException {
        int n = 0;
        for (int i = files.length - 1; i >= 0; --i) {
            if ((files[i] == null) || files[i].getName().endsWith(INDEX_FILE_EXT)) {
                continue;
            }

            String name = files[i].getName();
            String ext = name.substring(FILE_NAME_LENGTH);
            String newName = formatFileName(null, n, ext);

            Path newFile = dir.resolve(newName);
            Path indexFile = getIndexFile(files[i].toPath());

            if (Files.exists(indexFile)) {
                Files.move(indexFile,
                        FileUtils.getIndexFile(newFile),
                        StandardCopyOption.ATOMIC_MOVE);
            }

            Files.move(files[i].toPath(), newFile, StandardCopyOption.ATOMIC_MOVE);
            ++n;
        }
    }
}
