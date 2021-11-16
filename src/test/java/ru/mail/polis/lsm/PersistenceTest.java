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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.*;
import static ru.mail.polis.lsm.Utils.*;

class PersistenceTest {
    @Test
    void fs(@TempDir Path data) throws IOException {
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.upsert(Record.of(key(1), value(1), System.currentTimeMillis()));
        }

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            Iterator<Record> range = dao.range(null, null);
            assertTrue(range.hasNext());

            Record record = range.next();
            assertEquals(key(1), record.getKey());
            assertEquals(value(1), record.getValue());
        }

        recursiveDelete(data);

        assertFalse(Files.exists(data));
        Files.createDirectory(data);

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            assertFalse(dao.range(null, null).hasNext());
        }
    }

    @Test
    void remove(@TempDir Path data) throws IOException {
        // Reference value
        ByteBuffer key = wrap("SOME_KEY");
        ByteBuffer value = wrap("SOME_VALUE");

        // Create dao and fill data
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.upsert(Record.of(key, value, System.currentTimeMillis()));
            Iterator<Record> range = dao.range(null, null);

            assertTrue(range.hasNext());
            assertEquals(value, range.next().getValue());
        }

        // Load data and check
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            Iterator<Record> range = dao.range(null, null);
            assertTrue(range.hasNext());
            assertEquals(value, range.next().getValue());

            // Remove data and flush
            dao.upsert(Record.tombstone(key, System.currentTimeMillis()));
        }

        // Load and check not found
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            Iterator<Record> range = dao.range(null, null);

            assertTrue(range.next().isTombstone());
        }
    }

    @Test
    void replaceWithClose(@TempDir Path data) throws Exception {
        ByteBuffer key = wrap("KEY");
        ByteBuffer value = wrap("VALUE_1");
        ByteBuffer value2 = wrap("VALUE_2");

        // Initial insert
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.upsert(Record.of(key, value, System.currentTimeMillis()));

            Iterator<Record> range = dao.range(null, null);
            assertTrue(range.hasNext());
            assertEquals(value, range.next().getValue());
        }

        // Reopen
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            Iterator<Record> range = dao.range(null, null);
            assertTrue(range.hasNext());
            assertEquals(value, range.next().getValue());

            // Replace
            dao.upsert(Record.of(key, value2, System.currentTimeMillis()));

            Iterator<Record> range2 = dao.range(null, null);
            assertTrue(range2.hasNext());
            assertEquals(value2, range2.next().getValue());
        }

        // Reopen
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            // Last value should win
            Iterator<Record> range2 = dao.range(null, null);
            assertTrue(range2.hasNext());
            assertEquals(value2, range2.next().getValue());
        }
    }

    @Test
    void burn(@TempDir Path data) throws IOException {
        ByteBuffer key = wrap("FIXED_KEY");

        int overwrites = 100;
        for (int i = 0; i < overwrites; i++) {
            ByteBuffer value = value(i);
            try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
                dao.upsert(Record.of(key, value, System.currentTimeMillis()));
                assertEquals(value, dao.range(key, null).next().getValue());
            }

            // Check
            try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
                assertEquals(value, dao.range(key, null).next().getValue());
            }
        }
    }

    @Disabled
    @Test
    void hugeRecords(@TempDir Path data) throws IOException {
        // Reference value
        int size = 1024 * 1024;
        byte[] suffix = sizeBasedRandomData(size);
        int recordsCount = (int) (TestDaoWrapper.MAX_HEAP * 5 / size);

        prepareHugeDao(data, recordsCount, suffix);

        // Check
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            Iterator<Record> range = dao.range(null, null);

            for (int i = 0; i < recordsCount; i++) {
                verifyNext(suffix, range, i);
            }

            assertFalse(range.hasNext());
        }
    }

    @Disabled
    @Test
    void hugeRecordsSearch(@TempDir Path data) throws IOException {
        // Reference value
        int size = 1024 * 1024;
        byte[] suffix = sizeBasedRandomData(size);
        int recordsCount = (int) (TestDaoWrapper.MAX_HEAP * 5 / size);

        prepareHugeDao(data, recordsCount, suffix);

        // Check
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            int searchStep = 4;

            for (int i = 0; i < recordsCount / searchStep; i++) {
                ByteBuffer keyFrom = keyWithSuffix(i * searchStep, suffix);
                ByteBuffer keyTo = keyWithSuffix(i * searchStep + searchStep, suffix);

//                System.out.println(i);

                Iterator<Record> range = dao.range(keyFrom, keyTo);
                for (int j = 0; j < searchStep; j++) {
                    verifyNext(suffix, range, i * searchStep + j);
                }
                assertFalse(range.hasNext());
            }
        }
    }

    @Test
    void burnAndCompact(@TempDir Path data) throws IOException {
        DAOConfig config = new DAOConfig(data, DAOConfig.DEFAULT_MEMORY_LIMIT, Integer.MAX_VALUE);

        Map<ByteBuffer, ByteBuffer> map = Utils.generateMap(0, 1);

        int overwrites = 100;
        for (int i = 0; i < overwrites; i++) {
            try (DAO dao = TestDaoWrapper.create(config)) {
                map.forEach((k, v) -> dao.upsert(Record.of(k, v, System.currentTimeMillis())));
            }

            // Check
            try (DAO dao = TestDaoWrapper.create(config)) {
                assertDaoEquals(dao, map);
            }
        }

        int beforeCompactSize = getDirSize(data);

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.compact();
            assertDaoEquals(dao, map);
        }

        // just for sure
        try (DAO dao = TestDaoWrapper.create(config)) {
            assertDaoEquals(dao, map);
        }

        int size = getDirSize(data);
        assertTrue(beforeCompactSize / 50 > size);
    }

    private int getDirSize(Path data) throws IOException {
        int[] size = new int[1];

        Files.walkFileTree(data, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                size[0] += (int) attrs.size();
                return FileVisitResult.CONTINUE;
            }
        });

        return size[0];
    }

    private void verifyNext(byte[] suffix, Iterator<Record> range, int index) {
        ByteBuffer key = keyWithSuffix(index, suffix);
        ByteBuffer value = valueWithSuffix(index, suffix);

        Record next = range.next();

        assertEquals(key, next.getKey());
        assertEquals(value, next.getValue());
    }

    private void prepareHugeDao(@TempDir Path data, int recordsCount, byte[] suffix) throws IOException {
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            for (int i = 0; i < recordsCount; i++) {
                ByteBuffer key = keyWithSuffix(i, suffix);
                ByteBuffer value = valueWithSuffix(i, suffix);

                dao.upsert(Record.of(key, value, System.currentTimeMillis()));
            }
        }
    }

}
