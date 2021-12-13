package ru.mail.polis.service.lucas_mbele;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class ThreadPoolExecutorUtil {
    private final Logger logger = LoggerFactory.getLogger(ThreadPoolExecutorUtil.class);
    private final int corePoolSize;
    private final static int maximumPoolSize = 4;
    private final long keepAliveTime;
    private final int capacity;
    private final ThreadPoolExecutor threadPoolExecutor;
    
    public static ThreadPoolExecutorUtil init() {
        return new ThreadPoolExecutorUtil(2,Long.MAX_VALUE,100);
    }

    public ThreadPoolExecutorUtil(int corePoolSize,long keepAliveTime,int capacity) {
        this.capacity = capacity;
        this.corePoolSize = corePoolSize;
        this.keepAliveTime = keepAliveTime;
        BlockingQueue<Runnable> blockingQueue = new ArrayBlockingQueue<>(this.capacity);
        threadPoolExecutor = new ThreadPoolExecutor(this.corePoolSize, this.maximumPoolSize, 
                                                    this.keepAliveTime, TimeUnit.NANOSECONDS, 
                                                    blockingQueue);
        threadPoolExecutor.prestartAllCoreThreads();
        handleRejections();
    }

    public void handleRejections() {
        threadPoolExecutor.setRejectedExecutionHandler((task, executor) -> {
            try {
                Thread.sleep(1000);
                logger.error("Task cannot be added...");
            } catch (InterruptedException e) {
                logger.error("Thread interrupted: ()", e.getCause());
                Thread.currentThread().interrupt();
            }
            threadPoolExecutor.execute(task);
        });
    }

    public void executeTask(Runnable task) {
            threadPoolExecutor.execute(task);
    }
}
