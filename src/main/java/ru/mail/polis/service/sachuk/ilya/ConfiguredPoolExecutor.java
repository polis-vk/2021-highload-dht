package ru.mail.polis.service.sachuk.ilya;

import ru.mail.polis.ThreadUtils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ConfiguredPoolExecutor {

    private final ExecutorConfig executorConfig;
    private final ExecutorService mainExecutor;
    private final BlockingQueue<Runnable> queue;

    public ConfiguredPoolExecutor(ExecutorConfig executorConfig) {
        this.executorConfig = executorConfig;

        this.queue = new LinkedBlockingQueue<>(executorConfig.queueSize);
        this.mainExecutor = new ThreadPoolExecutor(
                executorConfig.threadNumber,
                executorConfig.threadNumber,
                0L,
                TimeUnit.MILLISECONDS,
                queue
        );
    }

    public void execute(Runnable runnable) {
        mainExecutor.execute(runnable);
    }

    public <T> Future<T> submit(Callable<T> callable) {
        return mainExecutor.submit(callable);
    }

    public boolean isQueueFull() {
        return queue.size() >= executorConfig.queueSize;
    }

    public void close() {
        ThreadUtils.awaitForShutdown(mainExecutor);
    }
}
