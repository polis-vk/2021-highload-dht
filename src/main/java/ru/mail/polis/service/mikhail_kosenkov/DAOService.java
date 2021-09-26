package ru.mail.polis.service.mikhail_kosenkov;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class DAOService {

    private final DAO dao;

    public DAOService(DAO dao) {
        this.dao = dao;
    }

    public byte[] getEntity(String id) {
        ByteBuffer key = byteBufferFromString(id);
        Iterator<Record> range = dao.range(key, DAO.nextKey(key));
        if (range.hasNext()) {
            ByteBuffer value = range.next().getValue();
            return arrayFromByteBuffer(value);
        }
        return null;
    }

    public void putEntity(String id, byte[] entity) {
        ByteBuffer key = byteBufferFromString(id);
        dao.upsert(Record.of(key, ByteBuffer.wrap(entity)));
    }

    public void deleteEntity(String id) {
        ByteBuffer key = byteBufferFromString(id);
        dao.upsert(Record.tombstone(key));
    }

    private static ByteBuffer byteBufferFromString(String key) {
        return ByteBuffer.wrap(key.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] arrayFromByteBuffer(ByteBuffer byteBuffer) {
        byte[] arr = new byte[byteBuffer.remaining()];
        byteBuffer.get(arr);
        return arr;
    }

}
