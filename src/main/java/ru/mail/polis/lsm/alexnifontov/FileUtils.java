package ru.mail.polis.lsm.alexnifontov;

import java.nio.file.Path;

public final class FileUtils {
    private FileUtils() {
        // not instantiate
    }

    private static Path resolveWithExt(Path file, String ext) {
        return file.resolveSibling(file.getFileName() + ext);
    }

    static Path getIndexFile(Path file) {
        return resolveWithExt(file, ".idx");
    }

    static Path getTmpFile(Path file) {
        return resolveWithExt(file, ".tmp");
    }
}
