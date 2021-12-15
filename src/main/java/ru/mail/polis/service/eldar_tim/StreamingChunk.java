package ru.mail.polis.service.eldar_tim;

import one.nio.util.ByteArrayBuilder;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class StreamingChunk {

    public static final StreamingChunk EMPTY_INSTANCE = new StreamingChunk(0);
    public static final byte[] EMPTY_INSTANCE_BYTES = EMPTY_INSTANCE.bytes();

    private final byte[] delimiterBytes = "\r\n".getBytes(StandardCharsets.US_ASCII);

    private final ByteArrayBuilder builder;

    private StreamingChunk(int contentLength) {
        byte[] prefixBytes = Integer.toHexString(contentLength).getBytes(StandardCharsets.US_ASCII);
        int totalChunkLength = prefixBytes.length + contentLength + delimiterBytes.length * 2;

        builder = new ByteArrayBuilder(totalChunkLength);
        builder.append(prefixBytes);
        builder.append(delimiterBytes);
    }

    public static StreamingChunk init(int contentLength) {
        if (contentLength <= 0) {
            throw new IllegalArgumentException("Chunk content length <= 0");
        }
        return new StreamingChunk(contentLength);
    }

    public static StreamingChunk empty() {
        return EMPTY_INSTANCE;
    }

    public StreamingChunk append(ByteBuffer data, int length) {
        builder.append(data, length);
        return this;
    }

    public StreamingChunk append(char ch) {
        builder.append(ch);
        return this;
    }

    public byte[] bytes() {
        builder.append(delimiterBytes);
        return builder.buffer();
    }
}
