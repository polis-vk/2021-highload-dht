package ru.mail.polis.service.sachuk.ilya;

import ru.mail.polis.ThreadUtils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class RequestPoolExecutor {

    private final ExecutorConfig executorConfig;
    private final ExecutorService mainExecutor;
    private final ExecutorService helpExecutor;
    private final BlockingQueue<Runnable> queue;

    public RequestPoolExecutor(ExecutorConfig executorConfig) {
        this.executorConfig = executorConfig;

        this.queue = new LinkedBlockingQueue<>(executorConfig.queueSize);
        this.mainExecutor = new ThreadPoolExecutor(
                executorConfig.threadNumber,
                executorConfig.threadNumber,
                0L,
                TimeUnit.MILLISECONDS,
                queue
        );

        this.helpExecutor = Executors.newFixedThreadPool(10);
    }

    public void addTask(Runnable runnable) {
        mainExecutor.execute(runnable);
    }

    public boolean isQueueFull() {
        return queue.size() == executorConfig.queueSize;
    }

    public void executeNow(Runnable runnable) {
        helpExecutor.execute(runnable);
    }

    public void close() {
        ThreadUtils.awaitForShutdown(mainExecutor);
        ThreadUtils.awaitForShutdown(helpExecutor);
    }
}
