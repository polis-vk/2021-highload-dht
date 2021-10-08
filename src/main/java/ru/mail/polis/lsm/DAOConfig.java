package ru.mail.polis.lsm;

import java.nio.file.Path;

public class DAOConfig {
    public static final int DEFAULT_MEMORY_LIMIT = 4 * 1024 * 1024;
    public static final int DEFAULT_FLUSH_QUEUE_LIMIT = 4; // this * DEFAULT_MEMORY_LIMIT = MAX FLUSH LIMIT

    public final Path dir;
    public final int memoryLimit;
    public final int flushQueueLimit;

    public DAOConfig(Path dir) {
        this(dir, DEFAULT_MEMORY_LIMIT, DEFAULT_FLUSH_QUEUE_LIMIT);
    }

    public DAOConfig(Path dir, int memoryLimit, int flushQueueLimit) {
        this.dir = dir;
        this.memoryLimit = memoryLimit;
        this.flushQueueLimit = flushQueueLimit;
    }
}
