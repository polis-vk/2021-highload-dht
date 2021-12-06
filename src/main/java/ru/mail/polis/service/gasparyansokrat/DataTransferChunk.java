package ru.mail.polis.service.gasparyansokrat;

import ru.mail.polis.lsm.Record;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

final class DataTransferChunk {

    private final Iterator<Record> data;
    private Iterator<Record> current;
    private boolean isEnd;

    public static final String ENDS = "0\r\n\r\n";

    private DataTransferChunk(final Iterator<Record> data) {
        this.data = data;
        this.current = data;
        this.isEnd = false;
    }

    public static DataTransferChunk build(final Iterator<Record> data) {
        return new DataTransferChunk(data);
    }

    public Iterator<Record> getData() {
        return data;
    }

    public void refresh() {
        current = data;
        isEnd = false;
    }

    public byte[] getChunk() {
        if (isEnd) {
            return null;
        }
        CharBuffer chunk;
        if (current.hasNext()) {
            chunk = buildChunk(current.next());
        } else {
            chunk = endChunk();
            isEnd = true;
        }
        return char2byteBuffer(chunk.asReadOnlyBuffer()).array();
    }

    private ByteBuffer char2byteBuffer(final CharBuffer buffer) {
        ByteBuffer tmpBuffer = ByteBuffer.allocate(buffer.limit());
        tmpBuffer.put(buffer.toString().getBytes(StandardCharsets.UTF_8));
        return tmpBuffer.position(0);
    }

    private CharBuffer buildChunk(final Record record) {
        final String key = StandardCharsets.UTF_8.decode(record.getKey()).toString();
        final String value = StandardCharsets.UTF_8.decode(record.getValue()).toString();
        final String chunkSize = Integer.toHexString(key.length() + value.length() + 1);
        final int totalSize = chunkSize.length() + key.length() + value.length() + 5 * Byte.BYTES;
        CharBuffer tmpChunk = CharBuffer.allocate(totalSize);
        tmpChunk.put(chunkSize);
        tmpChunk.put("\r\n");
        tmpChunk.put(key);
        tmpChunk.put('\n');
        tmpChunk.put(value);
        tmpChunk.put("\r\n");
        return tmpChunk.position(0);
    }

    private CharBuffer endChunk() {
        CharBuffer tmpChunk = CharBuffer.allocate(ENDS.length());
        tmpChunk.put(ENDS);
        return tmpChunk.position(0);
    }

}
