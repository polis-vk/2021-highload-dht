package ru.mail.polis.lsm;

import java.nio.file.Path;

public class DAOConfig {
    public static final int DEFAULT_MEMORY_LIMIT = 4 * 1024 * 1024;
    public static final int TABLES_LIMIT = 128;

    public final Path dir;
    public final int memoryLimit;
    public final int tableLimit;

    public DAOConfig(Path dir) {
        this(dir, DEFAULT_MEMORY_LIMIT, TABLES_LIMIT);
    }

    public DAOConfig(Path dir, int memoryLimit) {
        this(dir, memoryLimit, TABLES_LIMIT);
    }

    /**
     * some doc.
     */
    public DAOConfig(Path dir, int memoryLimit, int maxTables) {
        this.tableLimit = maxTables;
        this.memoryLimit = memoryLimit;
        this.dir = dir;
    }

}
