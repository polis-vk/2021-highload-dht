package ru.mail.polis.lsm.eldar_tim;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class LimitedMemTable extends MemTable {

    private final int memoryLimit;
    private final AtomicInteger memoryReserved = new AtomicInteger();
    private final AtomicBoolean flushSignal = new AtomicBoolean();

    public LimitedMemTable(int id, int memoryLimit) {
        super(id);
        this.memoryLimit = memoryLimit;
    }

    /**
     * Резервирует размер в памяти перед записью,
     * если это возможно (выделено не больше {@link LimitedMemTable#memoryLimit}).
     *
     * @param recordSize размер добавляемой записи
     * @return true, если удалось выделить место под последующую запись, иначе память переполнена
     */
    public boolean reserveSize(int recordSize) {
        int v, sum;
        do {
            v = memoryReserved.get();
            sum = v + recordSize;
            if (sum > memoryLimit) {
                return false;
            }
        } while (!memoryReserved.compareAndSet(v, sum));
        return true;
    }

    /**
     * Атомарно осуществляет запрос на flush (освобождение памяти).
     * Запрос следует выполнять, когда не удалось получить резерв памяти методом {@link LimitedMemTable#reserveSize}.
     *
     * @param recordSize размер добавляемой записи
     * @return true, если получен эксклюзивный доступ на осуществление flush, иначе false
     */
    public boolean requestFlush(int recordSize) {
        if (flushSignal.compareAndSet(false, true)) {
            // Временно занимаем весь резерв памяти, чтобы удостовериться, что её
            // действительно нехватает при запросе на запись.
            int tmpMemoryReserved = memoryReserved.getAndSet(memoryLimit);
            if (tmpMemoryReserved + recordSize > memoryLimit) {
                return true;
            } else {
                memoryReserved.set(tmpMemoryReserved);
                flushSignal.set(false);
            }
        }
        return false;
    }
}
