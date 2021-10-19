package ru.mail.polis.service.sachuk.ilya;

public class ExecutorConfig {
    public static final int DEFAULT_THREAD_NUMBER = 16;
    public static final int DEFAULT_QUEUE_SIZE = 256;

    public final int threadNumber;
    public final int queueSize;

    public ExecutorConfig() {
        this(DEFAULT_THREAD_NUMBER, DEFAULT_QUEUE_SIZE);
    }

    public ExecutorConfig(int threadNumber, int queueSize) {
        this.threadNumber = threadNumber;
        this.queueSize = queueSize;
    }
}
