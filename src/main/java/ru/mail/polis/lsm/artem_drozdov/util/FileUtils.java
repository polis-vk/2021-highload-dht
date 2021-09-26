package ru.mail.polis.lsm.artem_drozdov.util;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

public final class FileUtils {
    private FileUtils() {
        // Not supposed to be instantiated
    }

    public static void rename(Path file, Path tmpFile) throws IOException {
        Files.deleteIfExists(file);
        Files.move(tmpFile, file, StandardCopyOption.ATOMIC_MOVE);
    }

    private static Path resolveWithExt(Path file, String ext) {
        return file.resolveSibling(file.getFileName() + ext);
    }

    public static Path getIndexFile(Path file) {
        return resolveWithExt(file, ".idx");
    }

    public static Path getTmpFile(Path file) {
        return resolveWithExt(file, ".tmp");
    }

    public static FileChannel openForWrite(Path tmpFileName) throws IOException {
        return FileChannel.open(
                tmpFileName,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
        );
    }
}
