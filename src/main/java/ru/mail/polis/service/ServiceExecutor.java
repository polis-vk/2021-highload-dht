package ru.mail.polis.service;

import one.nio.net.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.service.eldar_tim.NamedThreadFactory;
import ru.mail.polis.service.exceptions.ClientBadRequestException;
import ru.mail.polis.service.exceptions.ServerRuntimeException;
import ru.mail.polis.service.exceptions.ServiceOverloadException;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public interface ServiceExecutor {
    void execute(Session session, ExceptionHandler handler, ServiceRunnable runnable);

    void run(Session session, ExceptionHandler handler, ServiceRunnable runnable);

    void awaitAndShutdown();

    interface ExceptionHandler {
        void handleException(Session session, ServerRuntimeException e);
    }

    interface ServiceRunnable {
        void run() throws IOException;
    }
}

class ServiceExecutorImpl extends ThreadPoolExecutor implements ServiceExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceExecutorImpl.class);

    public ServiceExecutorImpl(String threadName, int defaultWorkers, int queueLimit) {
        super(defaultWorkers, defaultWorkers,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(queueLimit),
                new NamedThreadFactory(threadName, defaultWorkers));
    }

    @Override
    public void execute(Session session, ExceptionHandler handler, ServiceRunnable runnable) {
        if (getQueue().remainingCapacity() > 0) {
            try {
                execute(() -> run(session, handler, runnable));
            } catch (RejectedExecutionException e) {
                handler.handleException(session, new ServiceOverloadException(e));
            }
        } else {
            handler.handleException(session, new ServiceOverloadException());
        }
    }

    @Override
    public void run(Session session, ExceptionHandler handler, ServiceRunnable runnable) {
        try {
            runnable.run();
        } catch (ServerRuntimeException e) {
            handler.handleException(session, e);
        } catch (NoSuchElementException e) {
            handler.handleException(session, new ClientBadRequestException(e));
        } catch (IOException | RuntimeException e) {
            handler.handleException(session, new ServerRuntimeException(e));
        }
    }

    @Override
    public void awaitAndShutdown() {
        try {
            shutdown();
            if (!awaitTermination(1, TimeUnit.MINUTES)) {
                shutdownNow();
                if (!awaitTermination(1, TimeUnit.MINUTES)) {
                    throw new InterruptedException();
                }
            }
        } catch (InterruptedException e) {
            LOG.error("Error: executor can't shutdown on its own", e);
            Thread.currentThread().interrupt();
        }
    }
}