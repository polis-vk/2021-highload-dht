package ru.mail.polis.lsm;

import java.nio.file.Path;

public class DAOConfig {
    public static final int DEFAULT_MEMORY_LIMIT_BYTES = 4 * 1024 * 1024;
    public static final int DEFAULT_FLUSH_FAIL_RETRY_TIME_MS = 5000;
    public static final int DEFAULT_COMPACT_THRESHOLD_TABLES = 4;

    public final Path dir;
    public final int memoryLimit;
    public final int flushRetryTimeMs;
    public final int compactThreshold;

    public DAOConfig(Path dir) {
        this(dir, DEFAULT_MEMORY_LIMIT_BYTES, DEFAULT_FLUSH_FAIL_RETRY_TIME_MS, DEFAULT_COMPACT_THRESHOLD_TABLES);
    }

    public DAOConfig(Path dir, int memoryLimit, int flushRetryTimeMs, int compactThreshold) {
        this.dir = dir;
        this.memoryLimit = memoryLimit;
        this.flushRetryTimeMs = flushRetryTimeMs;
        this.compactThreshold = compactThreshold;
    }
}
