package ru.mail.polis.service;

import one.nio.net.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.service.eldar_tim.NamedThreadFactory;
import ru.mail.polis.service.exceptions.ServiceOverloadException;
import ru.mail.polis.service.exceptions.ServiceRuntimeException;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public interface ServiceExecutor {
    void execute(Session session, ExceptionHandler handler, ServiceRunnable runnable);

    void awaitAndShutdown();

    interface ExceptionHandler {
        void handleException(Session session, ServiceRuntimeException e);
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
        try {
            execute(() -> {
                try {
                    runnable.run();
                } catch (IOException e) {
                    handler.handleException(session, new ServiceRuntimeException(e));
                } catch (ServiceRuntimeException e) {
                    handler.handleException(session, e);
                }
            });
        } catch (RejectedExecutionException e) {
            handler.handleException(session, new ServiceOverloadException(e));
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
            LOG.error("error: executor can't shutdown on its own", e);
            Thread.currentThread().interrupt();
        }
    }
}