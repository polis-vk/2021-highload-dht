package ru.mail.polis.lsm.igorsamokhin;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

public class LongMappedByteBuffer {
    private final ByteBuffer[] buffers;
    private final int[] sizes;
    private long position;
    private long limit;
    private final long capacity;

    private static final Method CLEAN;

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

    @SuppressWarnings({"PMD.ArrayIsStoredDirectly", // This is a wrapper of mapped byte buffer, so I need to do it.
            "PMD.AvoidArrayLoops"}) // In array loop I store capacity of array item. Not an array item.
    public LongMappedByteBuffer(ByteBuffer... buffers) {
        this.sizes = new int[buffers.length];
        long sum = 0;

        for (int i = 0; i < buffers.length; i++) {
            sizes[i] = buffers[i].capacity();
            sum += sizes[i];
        }

        this.position = 0;
        this.buffers = buffers;
        this.capacity = sum;
        limit(sum);
    }

    public LongMappedByteBuffer duplicate() {
        return asReadOnlyBuffer();
    }

    /**
     * Работает только с выровненными данными.
     */
    @SuppressWarnings("PMD.AvoidReassigningParameters")
    public ByteBuffer cut(int size) {
        Pair indexAndOffset = getIndexAndOffset(position);
        long offset = indexAndOffset.offset;
        int index = indexAndOffset.index;
        ByteBuffer buffer = buffers[index];

        if (position - offset + size > Integer.MAX_VALUE) {
            size = 0;
        }
        int oldLimit = buffer.limit();
        buffer.limit(size);
        buffer.position((int) (position - offset));
        ByteBuffer result = buffer.slice().limit(size);
        buffer.limit(oldLimit);
        return result;
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

    public int getInt() {
        Pair indexAndOffset = getIndexAndOffset(position);
        long offset = indexAndOffset.offset;
        int index = indexAndOffset.index;
        ByteBuffer buffer = buffers[index];
        if ((position - offset + Integer.BYTES > buffer.capacity())) {
            if ((index + 1) >= buffers.length) {
                throw new BufferUnderflowException();
            }
            buffer = buffers[index + 1];
            buffer.position(0);
        } else {
            buffer.position((int) (position - offset));
        }
        int anInt = buffer.getInt();
        position += Integer.BYTES;
        return anInt;
    }

    public LongMappedByteBuffer slice() {
        Pair indexAndOffset = getIndexAndOffset(position);
        long offset = indexAndOffset.offset;
        int index = indexAndOffset.index;

        ByteBuffer slice = buffers[index].position((int) (position - offset)).slice();
        ByteBuffer[] returnBuffers = new ByteBuffer[buffers.length - index];
        returnBuffers[0] = slice;
        for (int i = index + 1; i < buffers.length; i++) {
            returnBuffers[i - index] = buffers[i].duplicate();
        }
        return new LongMappedByteBuffer(returnBuffers);
    }

    public long remaining() {
        return capacity - position;
    }

    public int mismatch(ByteBuffer keyToFind) {
        Pair indexAndOffset = getIndexAndOffset(position);
        long offset = indexAndOffset.offset;
        int index = indexAndOffset.index;
        ByteBuffer buffer = buffers[index];
        buffer.position((int) (position - offset));

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
        for (i = 0; i < sizes.length; i++) {
            if (sum + sizes[i] >= position) {
                break;
            }
            sum += sizes[i];
        }
        return new Pair(i, sum);
    }

    public LongMappedByteBuffer asReadOnlyBuffer() {
        ByteBuffer[] b = new ByteBuffer[buffers.length];
        for (int i = 0; i < buffers.length; i++) {
            b[i] = buffers[i].asReadOnlyBuffer();
        }
        return new LongMappedByteBuffer(b);
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
