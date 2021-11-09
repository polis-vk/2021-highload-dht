/*
 * Copyright 2021 (c) Odnoklassniki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.sql.Timestamp;

@SuppressWarnings("JavaLangClash")
public class Record {

    private static final short EMPTY = 0;
    private static final short USETIME = 1;
    private static final short TOMBSTONE = 2;

    private final ByteBuffer key;
    private final ByteBuffer value;

    public static final ByteBuffer dummy = ByteBuffer.allocate(1);

    Record(ByteBuffer key, @Nullable ByteBuffer value) {
        this.key = key.asReadOnlyBuffer();
        this.value = value == null ? null : value.asReadOnlyBuffer();
    }

    public static Record of(ByteBuffer key, ByteBuffer value) {
        return new Record(key.asReadOnlyBuffer(), buildValue(value, EMPTY, null).asReadOnlyBuffer());
    }

    public static Record of(ByteBuffer key, ByteBuffer value, final Timestamp time) {
        return new Record(key.asReadOnlyBuffer(), buildValue(value, USETIME, time).asReadOnlyBuffer());
    }

    public static Record direct(ByteBuffer key, ByteBuffer value) {
        return new Record(key.asReadOnlyBuffer(), value.asReadOnlyBuffer());
    }

    private static ByteBuffer buildValue(final ByteBuffer value, final short type, final Timestamp time) {
        final ByteBuffer curValue;
        switch (type) {
            case EMPTY:
                curValue = ByteBuffer.allocate(value.limit() + Short.BYTES);
                curValue.put(value.asReadOnlyBuffer());
                break;
            case USETIME:
            case TOMBSTONE:
                curValue = ByteBuffer.allocate(value.limit() + Long.BYTES + Short.BYTES);
                curValue.put(value.asReadOnlyBuffer());
                curValue.putLong(time.getTime());
                break;
            default:
                curValue = value;
                break;
        }
        curValue.putShort(type);
        return curValue;
    }

    public static Record tombstone(ByteBuffer key) {
        return new Record(key, buildValue(dummy, TOMBSTONE, new Timestamp(System.currentTimeMillis())));
    }

    public ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    public ByteBuffer getValue() {
        if (isEmpty() || isTombstone()) {
            return null;
        }
        ByteBuffer tmp = getValueBuffer();
        return (tmp == null) ? null : tmp.asReadOnlyBuffer();
    }

    public boolean isTombstone() {
        return (isEmpty() || getTypeBuffer(value) == TOMBSTONE);
    }

    public int getKeySize() {
        return key.remaining();
    }

    public int getValueSize() {
        return (isEmpty()) ? 0 : value.remaining();
    }

    private ByteBuffer getValueBuffer() {
        final ByteBuffer entireValue = value.duplicate();
        final short field = getTypeBuffer(entireValue);
        switch (field) {
            case EMPTY:
                entireValue.limit(value.limit() - Short.BYTES);
                entireValue.position(0);
                break;
            case USETIME:
                entireValue.limit(value.limit() - Short.BYTES - Long.BYTES);
                entireValue.position(0);
                break;
            default:
                return null;
        }
        return entireValue;
    }

    public byte[] getRawBytes() {
        byte[] buff = new byte[value.limit()];
        ByteBuffer rawbuff = value.duplicate();
        rawbuff.position(0).get(buff);
        return buff;
    }

    public byte[] getBytesValue() {
        ByteBuffer tmp = getValueBuffer();
        if (tmp == null) {
            return new byte[0];
        }
        byte[] buff = new byte[tmp.limit()];
        tmp.get(buff);
        return buff;
    }

    public Timestamp getTimestamp() {
        if (isEmpty()) {
            return null;
        }
        final ByteBuffer entireValue = value.duplicate();
        final short field = getTypeBuffer(entireValue);
        if (field != USETIME && field != TOMBSTONE) {
            return null;
        }

        final long time = entireValue.position(value.limit() - Long.BYTES - Short.BYTES).slice().getLong();
        return new Timestamp(time);
    }

    public ByteBuffer getRawValue() {
        return (isEmpty()) ? null : value.position(0).asReadOnlyBuffer();
    }

    private short getTypeBuffer(final ByteBuffer buffer) {
        return buffer.position(buffer.limit() - Short.BYTES).slice().getShort();
    }

    public boolean isEmpty() {
        return (value == null || value.limit() < Short.BYTES);
    }
}
