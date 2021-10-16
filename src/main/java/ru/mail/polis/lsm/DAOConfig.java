package ru.mail.polis.lsm;

import java.nio.file.Path;

public class DAOConfig {
    public static final int DEFAULT_MEMORY_LIMIT = 4 * 1024 * 1024;
    public static final int DEFAULT_FLUSH_FAIL_RETRY_TIME_MS = 500;

    public final Path dir;
    public final int memoryLimit;
    public final int flushRetryTimeMs;

    public DAOConfig(Path dir) {
        this(dir, DEFAULT_MEMORY_LIMIT, DEFAULT_FLUSH_FAIL_RETRY_TIME_MS);
    }

    public DAOConfig(Path dir, int memoryLimit, int flushRetryTimeMs) {
        this.dir = dir;
        this.memoryLimit = memoryLimit;
        this.flushRetryTimeMs = flushRetryTimeMs;
    }
}
