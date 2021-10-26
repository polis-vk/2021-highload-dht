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

import ru.mail.polis.lsm.artem_drozdov.DAOState;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class TestDaoWrapper implements DAO {
    static final long MAX_HEAP = 512 * 1024 * 1024;

    private final DAO delegate;

    private TestDaoWrapper(DAO delegate) {
        this.delegate = delegate;
    }

    public static DAO create(DAOConfig config) throws IOException {
        if (Runtime.getRuntime().maxMemory() > MAX_HEAP) {
            throw new IllegalStateException("The heap is too big. Consider setting Xmx.");
        }
        return new TestDaoWrapper(DAOFactory.create(config));
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return delegate.range(toReadOnly(fromKey), toReadOnly(toKey));
    }

    @Override
    public void upsert(Record record) {
        delegate.upsert(record);
    }

    @Override
    public void closeAndCompact() {
        delegate.closeAndCompact();
    }

    @Override
    public DAOState getState() {
        return DAOState.OK;
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    private ByteBuffer toReadOnly(@Nullable ByteBuffer key) {
        return key == null ? null : key.asReadOnlyBuffer();
    }

}
