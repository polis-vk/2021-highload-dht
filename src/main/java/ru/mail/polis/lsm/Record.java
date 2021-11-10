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

    private final ByteBuffer key;
    private final Value value;

    public static class Value {
        private final ByteBuffer value;
        private final long timestamp;

        public Value(@Nullable ByteBuffer value, long timestamp) {
            this.value = value == null ? null : value.asReadOnlyBuffer();
            this.timestamp = timestamp;
        }

        public Value(@Nullable ByteBuffer value) {
            this.value = value == null ? null : value.asReadOnlyBuffer();
            this.timestamp = System.currentTimeMillis();
        }

        public Value(Value value) {
            this.value = value.value == null ? null : value.value.asReadOnlyBuffer();
            this.timestamp = value.timestamp;
        }

        public static Value ofTombstone(long timestamp) {
            return new Value(null, timestamp);
        }

        public static Value ofTombstone() {
            return new Value(null, System.currentTimeMillis());
        }

        public boolean isTombstone() {
            return value == null;
        }

        public ByteBuffer get() {
            return value == null ? null : value.asReadOnlyBuffer();
        }

        public long timestamp() {
            return timestamp;
        }

        public int size() {
            return value == null ? 0 : value.remaining();
        }
    }

    Record(ByteBuffer key, ByteBuffer value) {
        this.key = key.asReadOnlyBuffer();
        this.value = new Value(value, System.currentTimeMillis());
    }

    Record(ByteBuffer key, Value value) {
        this.key = key.asReadOnlyBuffer();
        this.value = value;
    }

    public static Record of(ByteBuffer key, Value value) {
        return new Record(key.asReadOnlyBuffer(), value);
    }

    public static Record of(ByteBuffer key, ByteBuffer value) {
        return new Record(key.asReadOnlyBuffer(), new Value(value));
    }

    public static Record tombstone(ByteBuffer key) {
        return new Record(key, Value.ofTombstone());
    }

    public boolean isTombstone() {
        return value.isTombstone();
    }

    public ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    public Value getValue() {
        return value;
    }

    public int getKeySize() {
        return key.remaining();
    }
}
