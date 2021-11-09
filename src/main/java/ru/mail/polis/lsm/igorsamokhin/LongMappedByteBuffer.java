package ru.mail.polis.lsm.igorsamokhin;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

public class LongMappedByteBuffer {
    private final ByteBuffer[] buffers;
    private long position;
    private long limit;
    private final long capacity;

    private static final Method CLEAN;

    @SuppressWarnings({"PMD.ArrayIsStoredDirectly"}) // This is a wrapper of mapped byte buffer, so I need to do it.
    public LongMappedByteBuffer(ByteBuffer... buffers) {
        this.buffers = buffers;
        long sum = 0;

        for (ByteBuffer buffer : buffers) {
            sum += buffer.capacity();
        }

        this.capacity = sum;
        limit(sum);
    }

    public static void free(MappedByteBuffer buffer) throws IOException {
        try {
            CLEAN.invoke(null, buffer);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IOException(e);
        }
    }

    public void free() throws IOException {
        for (int i = 0; i < buffers.length; i++) {
            ByteBuffer buffer = buffers[i];
            free((MappedByteBuffer) buffer);
            buffers[i] = null;
        }
    }

    static {
        try {
            Class<?> name = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = name.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    public LongMappedByteBuffer duplicate() {
        return asReadOnlyBuffer();
    }

    /**
     * Копирует кусок ByteBuffer, равный size.
     * Для текущего position выбирается ByteBuffer.
     * Для него если position + size > capacity, то возвращается пустой буфер.
     */
    @SuppressWarnings("PMD.AvoidReassigningParameters")
    public ByteBuffer cut(int size) {
        long pos = position();
        Pair indexAndOffset = getIndexAndOffset(pos);
        long offset = indexAndOffset.offset;
        int index = indexAndOffset.index;
        ByteBuffer buffer = getBufferWithPositioning(index, offset, pos);

        if (pos - offset + size > buffer.capacity()) {
            throw new IllegalArgumentException("Illegal size");
        }

        return buffer.slice().limit(size);
    }

    public LongMappedByteBuffer limit(long newLimit) {
        if (newLimit > capacity || newLimit < 0) {
            throw new IllegalArgumentException("newPosition: " + newLimit + " limit: " + capacity);
        }
        limit = newLimit;
        if (position > limit) {
            position = limit;
        }

        return this;
    }

    public void position(long newPosition) {
        if ((newPosition > limit) || (newPosition < 0)) {
            throw new IllegalArgumentException("newPosition: " + newPosition + " limit: " + limit);
        }
        position = newPosition;
    }

    public long position() {
        return position;
    }

    private ByteBuffer getByteBufferToReadBytes(long pos, int bytes) {
        Pair indexAndOffset = getIndexAndOffset(pos);
        long offset = indexAndOffset.offset;
        int index = indexAndOffset.index;
        ByteBuffer buffer = getBuffer(index);
        if ((pos - offset + bytes > buffer.capacity())) {
            // assume that all data is completely in one buffer
            throw new IndexOutOfBoundsException(String.format("pos: %s, off: %s, cap: %s, i: %s, buffer[i].cap: %s",
                    pos, offset, capacity, index, buffer.capacity()));
        } else {
            buffer.position((int) (pos - offset));
        }
        return buffer;
    }

    public int getInt() {
        long pos = position();
        ByteBuffer buffer = getByteBufferToReadBytes(pos, Integer.BYTES);
        int anInt = buffer.getInt();
        position(pos + Integer.BYTES);
        return anInt;
    }

    public long getLong() {
        long pos = position();
        ByteBuffer buffer = getByteBufferToReadBytes(pos, Long.BYTES);
        long result = buffer.getLong();
        position(pos + Long.BYTES);
        return result;
    }

    public long remaining() {
        return limit - position;
    }

    public int mismatch(ByteBuffer keyToFind) {
        long pos = position();
        Pair indexAndOffset = getIndexAndOffset(pos);
        long offset = indexAndOffset.offset;
        int index = indexAndOffset.index;
        ByteBuffer buffer = getBufferWithPositioning(index, offset, pos);

        return buffer.mismatch(keyToFind);
    }

    public byte get(long position) {
        Pair indexAndOffset = getIndexAndOffset(position);
        long offset = indexAndOffset.offset;
        int index = indexAndOffset.index;

        return buffers[index].get((int) (position - offset));
    }

    public long capacity() {
        return capacity;
    }

    public Pair getIndexAndOffset(long position) {
        long sum = 0;
        int i;
        for (i = 0; i < buffers.length; i++) {
            int cap = buffers[i].capacity();
            if (sum + cap > position) {
                break;
            }
            sum += cap;
        }
        return new Pair(i, sum);
    }

    public LongMappedByteBuffer asReadOnlyBuffer() {
        ByteBuffer[] b = new ByteBuffer[buffers.length];
        for (int i = 0; i < buffers.length; i++) {
            b[i] = getBuffer(i);
        }
        return new LongMappedByteBuffer(b);
    }

    private ByteBuffer getBufferWithPositioning(int index, long offset, long position) {
        return getBuffer(index).position((int) (position - offset));
    }

    private ByteBuffer getBuffer(int index) {
        return buffers[index].asReadOnlyBuffer();
    }

    private static class Pair {
        public final int index;
        public final long offset;

        public Pair(int index, long offset) {
            this.index = index;
            this.offset = offset;
        }
    }
}
