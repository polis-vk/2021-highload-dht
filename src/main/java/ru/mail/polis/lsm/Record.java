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

@SuppressWarnings("JavaLangClash")
public class Record {

    private final long timeStamp;
    private final ByteBuffer key;
    private final ByteBuffer value;

    Record(ByteBuffer key, @Nullable ByteBuffer value, long timeStamp) {
        this.key = key.asReadOnlyBuffer();
        this.value = value == null ? null : value.asReadOnlyBuffer();
        this.timeStamp = timeStamp;
    }

    public static Record of(ByteBuffer key, ByteBuffer value) {
        return new Record(key.asReadOnlyBuffer(), value.asReadOnlyBuffer(), -1);
    }

    public static Record of(ByteBuffer key, ByteBuffer value, long timeStamp) {
        return new Record(key.asReadOnlyBuffer(), value.asReadOnlyBuffer(), timeStamp);
    }

    public static Record tombstone(ByteBuffer key) {
        return new Record(key, null, -1);
    }

    public static Record tombstone(ByteBuffer key, long timeStamp) {
        return new Record(key, null, timeStamp);
    }

    public ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    public ByteBuffer getValue() {
        return value == null ? null : value.asReadOnlyBuffer();
    }

    public boolean isTombstone() {
        return value == null;
    }

    public int getKeySize() {
        return key.remaining();
    }

    public int getValueSize() {
        return value == null ? 0 : value.remaining();
    }

    public int compareKeyWith(Record r) {
        return this.key.compareTo(r.key); //много памяти используется для того, чтобы достать ключи и сравнить их
    }

    public int compareValueWith(Record r) {
        return this.value.compareTo(r.value); //много памяти используется для того, чтобы достать ключи и сравнить их
    }

    public int size() {
        return getKeySize() + getValueSize() + Long.BYTES;
    }

    public long getTimeStamp() {
        return timeStamp;
    }
}
