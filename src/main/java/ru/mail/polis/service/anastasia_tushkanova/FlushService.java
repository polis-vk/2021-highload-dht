package ru.mail.polis.service.anastasia_tushkanova;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.artem_drozdov.SSTable;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class FlushService implements Closeable {
    private final static Logger log = LoggerFactory.getLogger(FlushService.class);

    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final DAOConfig config;
    private final BlockingQueue<QueueElement> tablesQueue = new LinkedBlockingQueue<>();
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final Consumer<SSTable> flushedTableConsumer;

    private int tablesCount;

    public FlushService(DAOConfig config, int initialTablesCount, Consumer<SSTable> flushedTableConsumer) {
        this.config = config;
        this.tablesCount = initialTablesCount;
        this.flushedTableConsumer = flushedTableConsumer;
        executorService.submit(this::run);
    }

    @Override
    public void close() throws IOException {
        try {
            executorService.shutdown();
            while (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Couldn't terminate flush service in 10s");
            }
            log.info("Successfully closed flush service");
        } catch (InterruptedException e) {
            log.error("Closing flush service was interrupted");
        }
    }

    public void submit(NavigableMap<ByteBuffer, Record> storage) {
        tablesQueue.add(new QueueElement(storage));
    }

    public List<NavigableMap<ByteBuffer, Record>> getWaitingToFlushStorages() {
        return tablesQueue.stream().map(QueueElement::getMemoryStorage).collect(Collectors.toList());
    }

    public void resetTablesCount() {
        tablesCount = 1;
    }

    private void run() {
        try {
            while (!tablesQueue.isEmpty() || !Thread.currentThread().isInterrupted()) {
                try {
                    QueueElement queueElement = tablesQueue.take();
                    Path dir = config.dir;
                    Path file = dir.resolve(SSTable.SSTABLE_FILE_PREFIX + tablesCount);
                    SSTable ssTable = SSTable.write(queueElement.memoryStorage.values().iterator(), file);
                    tablesCount++;
                    flushedTableConsumer.accept(ssTable);
                } catch (InterruptedException e) {
                    log.info("Interrupted while taking element from queue");
                }
            }
        } catch (IOException e) {
            log.error("Exception while running flush service [{}]", e);
        }
    }

    private static class QueueElement {
        private final NavigableMap<ByteBuffer, Record> memoryStorage;

        public QueueElement(NavigableMap<ByteBuffer, Record> memoryStorage) {
            this.memoryStorage = memoryStorage;
        }

        public NavigableMap<ByteBuffer, Record> getMemoryStorage() {
            return memoryStorage;
        }

    }
}
