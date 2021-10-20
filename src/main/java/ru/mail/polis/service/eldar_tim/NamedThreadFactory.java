package ru.mail.polis.service.eldar_tim;

import javax.annotation.Nonnull;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {
    
    private final String threadName;
    private final int totalThreads;

    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final ThreadFactory delegate;

    public NamedThreadFactory(String threadName) {
        this(threadName, 0);
    }

    public NamedThreadFactory(String threadName, int totalThreads) {
        this.threadName = threadName;
        this.totalThreads = totalThreads;
        delegate = Executors.defaultThreadFactory();
    }

    @Override
    public Thread newThread(@Nonnull Runnable r) {
        String name = buildName(this.threadName, threadNumber.getAndIncrement(), totalThreads);
        Thread t = delegate.newThread(r);
        t.setName(name);
        return t;
    }

    public static String buildName(String threadName, int threadNumber, int totalThreads) {
        StringBuilder sb = new StringBuilder();
        sb.append(threadName);
        if (totalThreads > 1) {
            sb.append(' ').append(threadNumber);
            sb.append('/').append(totalThreads);
        }
        return sb.toString();
    }
}