package ru.mail.polis.service.lucas_mbele;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class ThreadPoolExecutorUtil {

    private Logger logger = LoggerFactory.getLogger(ThreadPoolExecutorUtil.class);
    private final int corePoolSize;
    private final int maximumPoolSize = 4;
    private final long keepAliveTime;
    private final int capacity;
    private ThreadPoolExecutor threadPoolExecutor;


    public static ThreadPoolExecutorUtil init(){
        return new ThreadPoolExecutorUtil(2,Long.MAX_VALUE,1000);
    }

    public ThreadPoolExecutorUtil(int corePoolSize,long keepAliveTime,int capacity) {
        this.capacity = capacity;
        this.corePoolSize = corePoolSize;
        this.keepAliveTime = keepAliveTime;
        BlockingQueue<Runnable> blockingQueue =  new ArrayBlockingQueue<>(this.capacity);
        threadPoolExecutor = new ThreadPoolExecutor(this.corePoolSize, this.maximumPoolSize, this.keepAliveTime, TimeUnit.NANOSECONDS, blockingQueue);
        threadPoolExecutor.prestartAllCoreThreads();
        handleRejections();
    }

    public void handleRejections(){
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

    public void executeTask(Runnable task){

            threadPoolExecutor.execute(task);
            //logger.info("Queue size....{}", threadPoolExecutor.getQueue().size());
            //logger.info("Number of Active Threads ....{}", threadPoolExecutor.getActiveCount());
    }
}
