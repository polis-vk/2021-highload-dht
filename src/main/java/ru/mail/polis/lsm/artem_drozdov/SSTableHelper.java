package ru.mail.polis.lsm.artem_drozdov;

import ru.mail.polis.lsm.Record;

public class SSTableHelper {
    public static final String SSTABLE_FILE_PREFIX = "file_";
    public static final String COMPACTION_FILE_NAME = "compaction";
    public static final int MAX_BUFFER_SIZE = 4096;

    private SSTableHelper() {
    }

    public static int sizeOf(Record record) {
        int keySize = Integer.BYTES + record.getKeySize();
        int valueSize = Integer.BYTES + record.getValueSize();
        return keySize + valueSize;
    }
}
