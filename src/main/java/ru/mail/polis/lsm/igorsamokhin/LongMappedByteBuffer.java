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
        for (ByteBuffer buffer : buffers) {
            free((MappedByteBuffer) buffer);
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

        this.buffers = buffers;
        this.capacity = sum;
        limit(sum);
    }

    public LongMappedByteBuffer duplicate() {
        return new LongMappedByteBuffer(buffers);
    }

    /**
     * Return the index of ByteBuffer pointed to by position.
     */
    private int getIndex(long position) {
        long sum = 0;
        int i;
        for (i = 0; i < sizes.length; i++) {
            if (sum + sizes[i] >= position) {
                break;
            }
            sum += sizes[i];
        }
        return i;
    }

    /**
     * Работает только с выровненными данными.
     */
    @SuppressWarnings("PMD.AvoidReassigningParameters")
    public ByteBuffer cut(int size) {
        long offset = getOffset(position);
        int index = getIndex(position);
        ByteBuffer buffer = buffers[index];

        if (position - offset + size > Integer.MAX_VALUE) {
            size = 0;
        }

        buffer.position((int) (position - offset));
        return buffer.slice().limit((int) (position - offset + size));
    }

    public LongMappedByteBuffer limit(long newLimit) {
        if (newLimit > capacity || newLimit < 0) {
            throw createLimitException(newLimit);
        }
        limit = newLimit;
        if (position > limit) {
            position = limit;
        }

        return this;
    }

    private IllegalArgumentException createLimitException(long newLimit) {
        String msg;

        if (newLimit > capacity) {
            msg = "newLimit > capacity: (" + newLimit + " > " + capacity + ")";
        } else { // assume negative
            assert newLimit < 0 : "newLimit expected to be negative";
            msg = "newLimit < 0: (" + newLimit + " < 0)";
        }

        return new IllegalArgumentException(msg);
    }

    public void position(long newPosition) {
        if ((newPosition > limit) || (newPosition < 0)) {
            throw createPositionException(newPosition);
        }
        position = newPosition;
    }

    private long getOffset(long position) {
        long sum = 0;
        for (int size : sizes) {
            if (sum + size >= position) {
                break;
            }
            sum += size;
        }
        return sum;
    }

    private IllegalArgumentException createPositionException(long newPosition) {
        String msg;

        if (newPosition > limit) {
            msg = "newPosition > limit: (" + newPosition + " > " + limit + ")";
        } else { // assume negative
            assert newPosition < 0 : "newPosition expected to be negative";
            msg = "newPosition < 0: (" + newPosition + " < 0)";
        }

        return new IllegalArgumentException(msg);
    }

    public long position() {
        return position;
    }

    public int getInt() {
        int index = getIndex(position);
        ByteBuffer buffer = buffers[index];
        if ((position + Integer.BYTES > buffer.capacity())) {
            if ((index + 1) >= buffers.length) {
                throw new BufferUnderflowException();
            }
            buffer = buffers[index + 1];
            buffer.position(0);
        } else {
            buffer.position((int) (position - getOffset(position)));
        }
        int anInt = buffer.getInt();
        position += Integer.BYTES;
        return anInt;
    }

    public LongMappedByteBuffer slice() {
        int index = getIndex(position);
        long offset = getOffset(position);

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
        int index = getIndex(position);
        long offset = getOffset(position);
        ByteBuffer buffer = buffers[index];
        buffer.position((int) (position - offset));

        return buffer.mismatch(keyToFind);
    }

    public byte get(long position) {
        //todo придумать здесь что-нибудь, чтобы не высчитывать сумму дважды
        int index = getIndex(position);
        long offset = getOffset(position);

        return buffers[index].get((int) (position - offset));
    }
}
