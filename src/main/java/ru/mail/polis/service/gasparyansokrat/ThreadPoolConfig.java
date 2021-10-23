package ru.mail.polis.service.gasparyansokrat;

import java.util.concurrent.TimeUnit;

public final class ThreadPoolConfig {
    public final int poolSize;
    public final int queueSize;
    public final int keepAlive;
    public final TimeUnit unit;
    public static final int MAX_THREAD_POOL = 4;

    /**
     * Hey, doc.
     */
    public ThreadPoolConfig(final int poolSize, final int queueSize, final int keepAlive, final TimeUnit unit) {
        this.poolSize = poolSize;
        this.queueSize = queueSize;
        this.keepAlive = keepAlive;
        this.unit = unit;
    }

}