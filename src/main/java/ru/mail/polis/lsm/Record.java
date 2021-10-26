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
    private final ByteBuffer value;

    Record(ByteBuffer key, @Nullable ByteBuffer value) {
        this.key = key.asReadOnlyBuffer();
        this.value = value == null ? null : value.asReadOnlyBuffer();
    }

    public static Record of(ByteBuffer key, ByteBuffer value) {
        return new Record(key.asReadOnlyBuffer(), value.asReadOnlyBuffer());
    }

    public static Record tombstone(ByteBuffer key) {
        return new Record(key, null);
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
}
