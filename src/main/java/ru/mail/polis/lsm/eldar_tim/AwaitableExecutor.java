package ru.mail.polis.lsm.eldar_tim;

import one.nio.async.CompletedFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.InterruptedByTimeoutException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class AwaitableExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(AwaitableExecutor.class);

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private volatile Future<?> future = new CompletedFuture<>(null);

    public void await() {
        try {
            future.get();
        } catch (InterruptedException e) {
            LOG.error("Future await error: {}", e.getMessage(), e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            LOG.error("Future await error: {}", e.getMessage(), e);
        }
    }

    public void execute(Runnable runnable) {
        future = executor.submit(runnable);
    }

    public void shutdown() throws InterruptedByTimeoutException {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                LOG.error("Executor service was interrupted by timeout");
                throw new InterruptedByTimeoutException();
            }
        } catch (InterruptedException ie) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
