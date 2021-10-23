package ru.mail.polis.service.gasparyansokrat;

public interface ConsistentHash {

    /**
     * Возвращает ноду для заданного ключа в области определения - [0, 2*PI)
     */
    String getNode(final String key);
}
