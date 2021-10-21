package ru.er_log.experemental;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Main {

    private ExecutorService executorService = Executors.newFixedThreadPool(4);
    private ThreadPoolExecutor executor = new ThreadPoolExecutor(3, 3,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(3),
            Executors.defaultThreadFactory());

    public Main() {
        executor.setRejectedExecutionHandler((r, executor) -> System.err.println("Rejected"));

        for (int i = 0; i < 100; i++) {
            executor.execute(() -> {
                System.out.println(Thread.currentThread().getName());
                sleep(1);
            });
        }

//        List<Future<?>> futures = new ArrayList<>();
//
//        futures.add(executorService.submit(this::task1));
//        futures.add(executorService.submit(this::task2));
//
//        futures.forEach(f -> {
//            try {
//                f.get();
//            } catch (InterruptedException | ExecutionException e) {
//                e.printStackTrace();
//            }
//        });
//
//        executorService.shutdown();
    }

    private synchronized void task1() {
        System.out.println("Enter " + Thread.currentThread().getName());
        sleep(10000);
        System.out.println("Out from " + Thread.currentThread().getName());
    }

    private void task2() {
        System.out.println("Enter " + Thread.currentThread().getName());
        synchronized (this) {
            System.out.println("sync block ok" + Thread.currentThread().getName());
            sleep(5000);
        }
        System.out.println("Out from " + Thread.currentThread().getName());
    }

    private void common() {
        System.out.println("I'm in: " + Thread.currentThread().getName());

        synchronized (this) {
            System.out.println("I'm in SYNC: " + Thread.currentThread().getName());
            try {
                Thread.sleep(4000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("I'm out: " + Thread.currentThread().getName());
    }

    private void sleep(int timeMs) {
        try {
            Thread.sleep(timeMs);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new Main();
    }
}
