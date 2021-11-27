package ru.mail.polis.service.eldar_tim;

import one.nio.net.Session;
import ru.mail.polis.service.exceptions.ServerRuntimeException;

import java.io.IOException;
import java.util.concurrent.Executor;

public interface ServiceExecutor extends Executor {
    void execute(Session session, ExceptionHandler handler, ServiceRunnable runnable);

    void run(Session session, ExceptionHandler handler, ServiceRunnable runnable);

    boolean externalRequestExecute(int tasksNum);

    void externalMarkExecuted();

    void awaitAndShutdown();

    interface ExceptionHandler {
        void handleException(Session session, ServerRuntimeException e);
    }

    @FunctionalInterface
    interface ServiceRunnable {
        void run() throws IOException;
    }
}
