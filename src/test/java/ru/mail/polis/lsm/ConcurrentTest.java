package ru.mail.polis.lsm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConcurrentTest {

    private static final int THREAD_COUNT = 32;
    private static final int TASKS_COUNT = 1_000;

    private DAO dao;
    private ExecutorService executor;

    @BeforeEach
    void start(@TempDir Path dir) throws IOException {
        dao = TestDaoWrapper.create(new DAOConfig(dir));
        executor = Executors.newFixedThreadPool(THREAD_COUNT);
    }

    @AfterEach
    void finish() throws IOException {
        dao.close();
    }

    @Test
    @RepeatedTest(value = 100)
    void concurrent() throws InterruptedException, ExecutionException, TimeoutException {
        List<Future<?>> exceptionsTracker = new ArrayList<>(TASKS_COUNT);
        for (int i = 0; i < TASKS_COUNT; i++) {
            ByteBuffer key = ByteBuffer.wrap(("key" + i).getBytes(StandardCharsets.UTF_8));
            ByteBuffer value = ByteBuffer.wrap(("value" + i).getBytes(StandardCharsets.UTF_8));
            Record record = Record.of(key, value);
            Future<?> future = executor.submit(() -> {
                dao.upsert(record);
                Optional<Record> found = StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(dao.range(null, null), Spliterator.ORDERED),
                        false
                ).filter(r -> r.getKey().equals(record.getKey())).findAny();

                assertTrue(found.isPresent());
                assertEquals(record.getValue(), found.get().getValue());
            });
            exceptionsTracker.add(future);
        }

        executor.shutdown();
        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
            throw new InterruptedException();
        }

        assertEquals(TASKS_COUNT, exceptionsTracker.size());
        for (Future<?> future : exceptionsTracker) {
            future.get(1, TimeUnit.SECONDS);
        }
    }


}
