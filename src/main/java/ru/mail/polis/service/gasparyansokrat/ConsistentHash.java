package ru.mail.polis.service.gasparyansokrat;

import java.io.IOException;
import java.util.List;

public interface ConsistentHash {

    /**
     * Возвращает ноду для заданного ключа в области определения - [0, 2*PI).
     */
    String getNode(final String key);

    /**
     * Возвращает список уникальных узлов детерминированным образом.
     */
    List<String> getNodes(final String key, final int numNodes) throws IOException;
}
