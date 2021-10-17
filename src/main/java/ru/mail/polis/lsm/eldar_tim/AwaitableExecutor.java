package ru.mail.polis.lsm.eldar_tim;

import one.nio.async.CompletedFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.service.eldar_tim.NamedThreadFactory;

import java.nio.channels.InterruptedByTimeoutException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Однопоточный исполнитель задач с функциями
 * ожидания завершения задачи и самоперезапуска.
 */
public class AwaitableExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(AwaitableExecutor.class);

    private final ExecutorService executor;
    private final String executorName;

    private final AtomicReference<Future<?>> future = new AtomicReference<>();

    public AwaitableExecutor(String threadName) {
        executorName = threadName + " executor";
        executor = Executors.newSingleThreadExecutor(new NamedThreadFactory(threadName));
        future.set(new CompletedFuture<>(null));
    }

    /**
     * Отправляет задачу на исполнение.
     * Не является потокобезопасным методом.
     *
     * @param runnable задача на исполнение
     */
    public void execute(ContextRunnable runnable) {
        Runnable r = () -> runnable.run(new Context(runnable));
        future.set(executor.submit(r));
    }

    /**
     * Ожидает завершения исполняемого кода.
     * Потокобезопасно.
     * Задача может перезапустить себя, из-за чего метод
     * занимается перепроверкой её instance.
     */
    public void await() {
        Future<?> f;
        do {
            f = future.get();
            await(f);
        } while (f != future.get());
    }

    public boolean isDone() {
        return future.get().isDone();
    }

    public void shutdown() throws InterruptedByTimeoutException {
        LOG.info("Shutting down executor '{}'", executorName);
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                LOG.error("Executor service was interrupted due to timeout");
                throw new InterruptedByTimeoutException();
            }
        } catch (InterruptedException ie) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void await(Future<?> future) {
        try {
            future.get();
        } catch (CancellationException e) {
            LOG.info("Future was canceled: {}", e.getMessage());
        } catch (InterruptedException e) {
            LOG.error("Future await error", e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            LOG.error("Future await error", e);
        }
    }

    public class Context {
        private final ContextRunnable runnable;

        private Context(ContextRunnable runnable) {
            this.runnable = runnable;
        }

        /**
         * Отправляет поток в сон на время {@code millis}.
         *
         * @param millis время сна (мс)
         */
        public void sleep(int millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                LOG.error("Sleep interrupted for the executor's thread", e);
                Thread.currentThread().interrupt();
            }
        }

        /**
         * Перезапускает задачу.
         * Старая задача будет безопасно отменена.
         * Потоки, ожидающие завершения старой задачи станут ожидающими для новой.
         */
        public void relaunch() {
            LOG.info("Relaunching task in executor: '{}', previous task will be canceled", executorName);
            Future<?> oldFuture = future.get();
            future.set(executor.submit(() -> runnable.run(this)));
            oldFuture.cancel(true);
            throw new IllegalStateException("This state must be unreachable");
        }
    }

    public interface ContextRunnable {
        void run(Context context);
    }
}
