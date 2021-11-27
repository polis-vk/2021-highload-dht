package ru.mail.polis.service.eldar_tim;

import one.nio.net.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.service.exceptions.ClientBadRequestException;
import ru.mail.polis.service.exceptions.ServerRuntimeException;
import ru.mail.polis.service.exceptions.ServiceOverloadException;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Исполнитель задач с ограниченной очередью.
 */
public class LimitedServiceExecutor implements ServiceExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(LimitedServiceExecutor.class);

    private final int queueLimit;
    private final ExecutorService delegate;

    private final AtomicInteger queueSize = new AtomicInteger();

    public LimitedServiceExecutor(int queueLimit, ExecutorService delegate) {
        this.queueLimit = queueLimit;
        this.delegate = delegate;
    }

    public static ExecutorService createForkJoinPool(String threadName, int defaultWorkers) {
        return new ForkJoinPool(defaultWorkers, createForkJoinThreadFactory(threadName, defaultWorkers), null, true);
    }

    public static ExecutorService createFixedThreadPool(String threadName, int defaultWorkers) {
        return Executors.newFixedThreadPool(defaultWorkers, new NamedThreadFactory(threadName, defaultWorkers));
    }

    @Override
    public void execute(@Nonnull Runnable command) {
        if (LOG.isDebugEnabled()) {
            int size = queueSize.get();
            if (size > queueLimit + 1) {
                LOG.debug("Queue overflow: {} > {}", size, queueLimit);
            }
        }

        delegate.execute(command);
    }

    @Override
    public void execute(Session session, ExceptionHandler handler, ServiceRunnable runnable) {
        if (!requestExecute()) {
            handler.handleException(session, ServiceOverloadException.INSTANCE);
            return;
        }

        delegate.execute(() -> {
            try {
                run(session, handler, runnable);
            } finally {
                queueSize.decrementAndGet();
            }
        });
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
    public boolean externalRequestExecute(int tasksNum) {
        int v;
        do {
            v = queueSize.get();
            if (v + tasksNum > queueLimit) {
                return false;
            }
        } while (!queueSize.compareAndSet(v, v + tasksNum));
        return true;
    }

    @Override
    public void externalMarkExecuted() {
        queueSize.decrementAndGet();
    }

    @Override
    public void awaitAndShutdown() {
        try {
            delegate.shutdown();
            if (!delegate.awaitTermination(60, TimeUnit.SECONDS)) {
                delegate.shutdownNow();
                if (!delegate.awaitTermination(30, TimeUnit.SECONDS)) {
                    throw new InterruptedException();
                }
            }
        } catch (InterruptedException e) {
            LOG.error("Error: executor can't shutdown on its own", e);
            Thread.currentThread().interrupt();
        }
    }

    private boolean requestExecute() {
        int v;
        do {
            v = queueSize.get();
            if (v > queueLimit) {
                return false;
            }
        } while (!queueSize.compareAndSet(v, v + 1));
        return true;
    }

    private static ForkJoinPool.ForkJoinWorkerThreadFactory createForkJoinThreadFactory(
            String threadName, int defaultWorkers
    ) {
        return new ForkJoinPool.ForkJoinWorkerThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
                String name = NamedThreadFactory.buildName(threadName, threadNumber.getAndIncrement(), defaultWorkers);
                ForkJoinWorkerThread t = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                t.setName(name);
                return t;
            }
        };
    }
}
