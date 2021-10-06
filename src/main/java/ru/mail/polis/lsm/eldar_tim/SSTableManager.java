package ru.mail.polis.lsm.eldar_tim;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Пишет MemTable в SSTable в неблокирующем режиме.
 * Также асинхронно осуществляет склеивание SSTable в одну.
 */
public class SSTableManager {
    private final ConcurrentLinkedQueue<MemTable> unsavedMemTables = new ConcurrentLinkedQueue<>();

    private final ExecutorService EXECUTOR = Executors.newFixedThreadPool(2);

    public void scheduleFlush(MemTable storage) {
        unsavedMemTables.add(storage);
        scheduleFlush();
    }

    private void scheduleFlush() {
        EXECUTOR.submit(() -> {
            MemTable memTable = unsavedMemTables.poll();
        });
    }
}
