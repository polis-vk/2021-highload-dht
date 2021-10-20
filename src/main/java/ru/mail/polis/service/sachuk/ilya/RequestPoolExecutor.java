package ru.mail.polis.service.sachuk.ilya;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class RequestPoolExecutor {

    private final ExecutorConfig executorConfig;
    private final ExecutorService mainExecutor;
    private final ExecutorService helpExecutor;
    private final BlockingQueue<Runnable> queue;
    private final AtomicBoolean isEnd = new AtomicBoolean(false);

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

        startExecutor();
    }

    private void startExecutor() {
        mainExecutor.execute(() -> {
            while (true) {
                Runnable runnable;
                try {
                    runnable = queue.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
                runnable.run();
            }
        });
    }

    public void addTask(Runnable runnable) {
        queue.add(runnable);
    }

    public boolean isQueueFull() {
        return queue.size() == executorConfig.queueSize;
    }

    public void executeNow(Runnable runnable) {
        helpExecutor.execute(runnable);
    }
}
