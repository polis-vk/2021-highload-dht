package ru.mail.polis.lsm.eldar_tim.components;

import ru.mail.polis.lsm.Record;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class LimitedMemTable extends AbstractMemTable {

    public final int memoryLimit;
    private final AtomicInteger memoryReserved = new AtomicInteger();

    private final AtomicInteger orders = new AtomicInteger();
    private final AtomicBoolean flushSignal = new AtomicBoolean();

    public LimitedMemTable(int id, int memoryLimit) {
        super(id);
        this.memoryLimit = memoryLimit;
    }

    /**
     * Резервирует размер в памяти перед записью,
     * если это возможно (лимит {@link LimitedMemTable#memoryLimit} ещё не превышен).
     * Операция потокобезопасна.
     *
     * @param recordSize размер добавляемой записи
     * @return true, если удалось выделить место под последующую запись, иначе память переполнена
     */
    public boolean reserveSize(int recordSize) {
        int v;
        do {
            v = memoryReserved.get();
            if (v >= memoryLimit) {
                return false;
            }
        } while (!memoryReserved.compareAndSet(v, v + recordSize));
        return true;
    }

    /**
     * Осуществляет запрос на предоставление права осуществить flush.
     * Запрос следует выполнять только тогда, когда не удалось получить
     * резерв памяти методом {@link LimitedMemTable#reserveSize}.
     * Операция потокобезопасна.
     *
     * @return true, если получен эксклюзивный доступ на осуществление flush, иначе false
     */
    public boolean requestFlush() {
        if (flushSignal.compareAndSet(false, true)) {
            if (memoryReserved.get() >= memoryLimit) {
                return true;
            }
            flushSignal.set(false);
        }
        return false;
    }

    @Override
    public Record put(ByteBuffer key, Record value) {
        Record r = map.put(key, value);
        orders.decrementAndGet();
        return r;
    }
}
