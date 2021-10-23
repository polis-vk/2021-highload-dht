package ru.mail.polis.service.eldar_tim;

import one.nio.net.Session;
import ru.mail.polis.service.exceptions.ServerRuntimeException;

import java.io.IOException;

public interface ServiceExecutor {
    void execute(Session session, ExceptionHandler handler, ServiceRunnable runnable);

    void run(Session session, ExceptionHandler handler, ServiceRunnable runnable);

    void awaitAndShutdown();

    interface ExceptionHandler {
        void handleException(Session session, ServerRuntimeException e);
    }

    @FunctionalInterface
    interface ServiceRunnable {
        void run() throws IOException;
    }
}
