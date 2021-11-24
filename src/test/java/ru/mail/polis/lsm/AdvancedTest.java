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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import static ru.mail.polis.lsm.Utils.*;

public class AdvancedTest {

    @Test
    void close(@TempDir Path data) throws IOException {
        Iterator<Record> iterator;

        Map<ByteBuffer, ByteBuffer> map = generateMap(0, 1000);

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.upsert(Record.of(key(1), value(1), System.currentTimeMillis()));

            map.forEach((k, v) -> dao.upsert(Record.of(k, v, System.currentTimeMillis())));

            iterator = dao.range(null, null, false);
        }

        assertEquals(iterator, new TreeMap<>(map).entrySet());
    }

}
