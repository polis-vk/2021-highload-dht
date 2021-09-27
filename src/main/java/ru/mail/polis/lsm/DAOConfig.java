package ru.mail.polis.lsm;

import java.nio.file.Path;

public class DAOConfig {
    public static final int DEFAULT_MEMORY_LIMIT = 2 * 1024 * 1024;

    public final Path dir;
    public final int memoryLimit;

    public DAOConfig(Path dir) {
        this(dir, DEFAULT_MEMORY_LIMIT);
    }

    public DAOConfig(Path dir, int memoryLimit) {
        this.dir = dir;
        this.memoryLimit = memoryLimit;
    }
}
