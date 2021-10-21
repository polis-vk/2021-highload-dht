/*
 * Copyright 2021 (c) Odnoklassniki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.mail.polis.lsm;

import java.nio.file.Path;

public class DAOConfig {
    public static final int DEFAULT_WORKERS = Runtime.getRuntime().availableProcessors();

    public static final int DEFAULT_MEMORY_LIMIT_BYTES = 4 * 1024 * 1024;
    public static final int DEFAULT_FLUSH_FAIL_RETRY_TIME_MS = 5000;
    public static final int DEFAULT_COMPACT_THRESHOLD_TABLES = 4;

    public final Path dir;
    public final int defaultWorkers;
    public final int memoryLimit;
    public final int flushRetryTimeMs;
    public final int compactThreshold;

    public DAOConfig(Path dir) {
        this(dir,
                DEFAULT_WORKERS,
                DEFAULT_MEMORY_LIMIT_BYTES,
                DEFAULT_FLUSH_FAIL_RETRY_TIME_MS,
                DEFAULT_COMPACT_THRESHOLD_TABLES);
    }

    public DAOConfig(Path dir, int defaultWorkers, int memoryLimit, int flushRetryTimeMs, int compactThreshold) {
        this.dir = dir;
        this.defaultWorkers = defaultWorkers;
        this.memoryLimit = memoryLimit;
        this.flushRetryTimeMs = flushRetryTimeMs;
        this.compactThreshold = compactThreshold;
    }
}
