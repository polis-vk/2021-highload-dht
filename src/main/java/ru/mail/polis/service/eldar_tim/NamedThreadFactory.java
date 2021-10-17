package ru.mail.polis.service.eldar_tim;

import javax.annotation.Nonnull;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {
    private final String name;
    private final int totalThreads;

    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final ThreadFactory delegate;

    public NamedThreadFactory(String threadName) {
        this(threadName, 0);
    }

    public NamedThreadFactory(String threadName, int totalThreads) {
        this.name = threadName;
        this.totalThreads = totalThreads;
        delegate = Executors.defaultThreadFactory();
    }

    @Override
    public Thread newThread(@Nonnull Runnable r) {
        StringBuilder sb = new StringBuilder();
        sb.append(name).append(' ').append(threadNumber.getAndIncrement());
        if (totalThreads > 0) {
            sb.append('/').append(totalThreads);
        }

        Thread t = delegate.newThread(r);
        t.setName(sb.toString());
        return t;
    }
}