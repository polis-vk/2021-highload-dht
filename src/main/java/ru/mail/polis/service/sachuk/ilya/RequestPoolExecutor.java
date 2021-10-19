package ru.mail.polis.service.sachuk.ilya;

import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class RequestPoolExecutor {

    private final ExecutorConfig executorConfig;
    private final ExecutorService executorService;
    private final Queue<RequestTask> queue;

    public RequestPoolExecutor(ExecutorConfig executorConfig) {
        this.executorConfig = executorConfig;

        this.executorService = Executors.newFixedThreadPool(executorConfig.threadNumber);
        this.queue = new LinkedBlockingQueue<>(executorConfig.queueSize);
//        executorService = new ThreadPoolExecutor();
    }

    public void addTask(RequestTask requestTask) {

    }

    public boolean isQueueFull() {
        return queue.size() == executorConfig.queueSize;
    }

}
