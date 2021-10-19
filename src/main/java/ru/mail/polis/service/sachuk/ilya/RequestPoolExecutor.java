package ru.mail.polis.service.sachuk.ilya;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class RequestPoolExecutor {

    private final ExecutorConfig executorConfig;
    private final ExecutorService executorService;
    private final BlockingQueue<Runnable> queue;

    public RequestPoolExecutor(ExecutorConfig executorConfig) {
        this.executorConfig = executorConfig;

        this.queue = new LinkedBlockingQueue<>(executorConfig.queueSize);
        this.executorService = new ThreadPoolExecutor(
                executorConfig.threadNumber,
                executorConfig.threadNumber,
                0L,
                TimeUnit.MILLISECONDS,
                queue
        );
    }

    public void addTask(Runnable runnable) {
        executorService.execute(runnable);
    }

    public boolean isQueueFull() {
        return queue.size() == executorConfig.queueSize;
    }
}
