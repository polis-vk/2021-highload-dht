package ru.mail.polis.lsm;

import java.nio.file.Path;

public class DAOConfig {
    public static final int DEFAULT_MEMORY_LIMIT = 4 * 1024 * 1024;
    public static final int MAX_NUMBER_OF_TABLES = 15;

    public int maxNumberOfTables;
    public final Path dir;
    public final int memoryLimit;

    public DAOConfig(Path dir) {
        this(dir, DEFAULT_MEMORY_LIMIT, MAX_NUMBER_OF_TABLES);
    }

    public DAOConfig(Path dir, int memoryLimit, int maxTables) {
        this.dir = dir;
        this.memoryLimit = memoryLimit;
        this.maxNumberOfTables = maxTables;
    }
}
