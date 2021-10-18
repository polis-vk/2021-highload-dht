package ru.mail.polis.service.sachuk.ilya;

import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

public class RequestPoolExecutor {

    private final ExecutorConfig executorConfig;
    private final ExecutorService executorService;
    private final Queue<RequestTask> queue = new LinkedBlockingDeque<>();

    public RequestPoolExecutor(ExecutorConfig executorConfig) {
        this.executorConfig = executorConfig;

        executorService = Executors.newFixedThreadPool(executorConfig.threadNumber);
//        executorService = new ThreadPoolExecutor();
    }

}
