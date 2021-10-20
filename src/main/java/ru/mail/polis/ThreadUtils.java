package ru.mail.polis;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public final class ThreadUtils {
    private ThreadUtils() {

    }

    public static void awaitForShutdown(ExecutorService executorService) {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                throw new IllegalStateException("Can't await for termination");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
