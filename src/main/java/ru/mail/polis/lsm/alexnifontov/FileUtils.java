package ru.mail.polis.lsm.alexnifontov;

import java.nio.file.Path;

public final class FileUtils {
    private FileUtils() {
        // not instantiate
    }

    public static Path resolveWithExt(Path file, String ext) {
        return file.resolveSibling(file.getFileName() + ext);
    }

    public static Path getIndexFile(Path file) {
        return resolveWithExt(file, ".idx");
    }

    public static Path getTmpFile(Path file) {
        return resolveWithExt(file, ".tmp");
    }
}
