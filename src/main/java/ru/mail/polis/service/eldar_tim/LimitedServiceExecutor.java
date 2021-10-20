package ru.mail.polis.service.eldar_tim;

import one.nio.net.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.service.exceptions.ClientBadRequestException;
import ru.mail.polis.service.exceptions.ServerRuntimeException;
import ru.mail.polis.service.exceptions.ServiceOverloadException;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LimitedServiceExecutor extends ForkJoinPool implements ServiceExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(LimitedServiceExecutor.class);

    private final AtomicInteger queueSize = new AtomicInteger();
    private final int queueLimit;

    public LimitedServiceExecutor(String threadName, int defaultWorkers, int queueLimit) {
        super(defaultWorkers, getThreadFactory(threadName, defaultWorkers),
                (t, e) -> {
                    throw new ServerRuntimeException(e);
                }, true);

        this.queueLimit = queueLimit;
    }

    @Override
    public void execute(Session session, ExceptionHandler handler, ServiceRunnable runnable) {
        if (!requestExecute()) {
            handler.handleException(session, new ServiceOverloadException());
            return;
        }

        try {
            execute(() -> {
                queueSize.decrementAndGet();
                run(session, handler, runnable);
            });
        } catch (RejectedExecutionException e) {
            handler.handleException(session, new ServiceOverloadException(e));
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

    private static ForkJoinWorkerThreadFactory getThreadFactory(String threadName, int defaultWorkers) {
        return new ForkJoinWorkerThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
                String name = NamedThreadFactory.buildName(threadName, threadNumber.getAndIncrement(), defaultWorkers);
                ForkJoinWorkerThread t = defaultForkJoinWorkerThreadFactory.newThread(pool);
                t.setName(name);
                return t;
            }
        };
    }
}
