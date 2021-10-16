package ru.mail.polis.service.eldar_tim;

import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public final class ServerUtils {

    private ServerUtils() {
        // Don't instantiate
    }

    public static void shutdownAndAwaitExecutor(ExecutorService executorService, Logger log) {
        try {
            executorService.shutdown();
            if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                executorService.shutdownNow();
                if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                    throw new InterruptedException();
                }
            }
        } catch (InterruptedException e) {
            log.error("error: executor can't shutdown on its own", e);
            Thread.currentThread().interrupt();
        }
    }
}
