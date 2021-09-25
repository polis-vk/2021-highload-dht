package ru.mail.polis.lsm;

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
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static ru.mail.polis.lsm.Utils.assertDaoEquals;
import static ru.mail.polis.lsm.Utils.getFilesCount;
import static ru.mail.polis.lsm.Utils.getFilesSize;
import static ru.mail.polis.lsm.Utils.key;
import static ru.mail.polis.lsm.Utils.keyWithSuffix;
import static ru.mail.polis.lsm.Utils.prepareHugeDao;
import static ru.mail.polis.lsm.Utils.recursiveDelete;
import static ru.mail.polis.lsm.Utils.sizeBasedRandomData;
import static ru.mail.polis.lsm.Utils.value;
import static ru.mail.polis.lsm.Utils.verifyNext;
import static ru.mail.polis.lsm.Utils.wrap;

class PersistenceTest {
    @Test
    void fs(@TempDir Path data) throws IOException {
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.upsert(Record.of(key(1), value(1)));
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
            dao.upsert(Record.of(key, value));
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
            dao.upsert(Record.tombstone(key));
        }

        // Load and check not found
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            Iterator<Record> range = dao.range(null, null);

            assertFalse(range.hasNext());
        }
    }

    @Test
    void replaceWithClose(@TempDir Path data) throws Exception {
        ByteBuffer key = wrap("KEY");
        ByteBuffer value = wrap("VALUE_1");
        ByteBuffer value2 = wrap("VALUE_2");

        // Initial insert
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.upsert(Record.of(key, value));

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
            dao.upsert(Record.of(key, value2));

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
                dao.upsert(Record.of(key, value));
                assertEquals(value, dao.range(key, null).next().getValue());
            }

            // Check
            try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
                assertEquals(value, dao.range(key, null).next().getValue());
            }
        }
    }

    @Test
    void hugeRecords(@TempDir Path data) throws IOException {
        // Reference value
        int size = 1024 * 1024;
        byte[] suffix = sizeBasedRandomData(size);
        int recordsCount = (int) (TestDaoWrapper.MAX_HEAP * 15 / size);

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

    @Test
    void hugeRecordsSearch(@TempDir Path data) throws IOException {
        // Reference value
        int size = 1024 * 1024;
        byte[] suffix = sizeBasedRandomData(size);
        int recordsCount = (int) (TestDaoWrapper.MAX_HEAP * 15 / size);

        prepareHugeDao(data, recordsCount, suffix);

        // Check
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            int searchStep = 4;
            for (int i = 0; i < recordsCount / searchStep; i++) {
                ByteBuffer keyFrom = keyWithSuffix(i * searchStep, suffix);
                ByteBuffer keyTo = keyWithSuffix(i * searchStep + searchStep, suffix);
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
        Map<ByteBuffer, ByteBuffer> map = Utils.generateMap(0, 1);

        int overwrites = 100;
        for (int i = 0; i < overwrites; i++) {
            try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
                map.forEach((k, v) -> dao.upsert(Record.of(k, v)));
            }

            // Check
            try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
                assertDaoEquals(dao, map);
            }
        }

        int beforeCompactSize = getDirSize(data);

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.closeAndCompact();
            assertDaoEquals(dao, map);
        }

        // just for sure
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            assertDaoEquals(dao, map);
        }

        int size = getDirSize(data);
        assertTrue(beforeCompactSize / 50 > size);
    }

    @Test
    void compactEmpty(@TempDir Path data) throws IOException {
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.closeAndCompact();
            Iterator<Record> range = dao.range(null, null);
            assertFalse(range.hasNext());
            assertEquals(0, getFilesCount(data));
        }
        assertEquals(0, getFilesCount(data));
    }

    @Test
    void compactTombstones(@TempDir Path data) throws IOException {
        TreeMap<ByteBuffer, ByteBuffer> expectedData = new TreeMap<>();
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            for (int i = 0; i < 20; i++) {
                ByteBuffer key = wrap("key_" + i);
                ByteBuffer value = wrap("value" + i);
                dao.upsert(Record.of(key, value));
                expectedData.put(key, value);
            }
            for (int i = 5; i < 15; i += 2) {
                ByteBuffer key = wrap("key_" + i);
                dao.upsert(Record.tombstone(key));
                expectedData.remove(key);
            }
        }

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            for (int i = 20; i < 50; i += 4) {
                ByteBuffer key = wrap("key_" + i);
                ByteBuffer value = wrap("value" + i);
                dao.upsert(Record.of(key, value));
                expectedData.put(key, value);
            }
            for (int i = 20; i < 50; i += 8) {
                ByteBuffer key = wrap("key_" + i);
                dao.upsert(Record.tombstone(key));
                expectedData.remove(key);
            }
        }

        long filesSizeBefore = getFilesSize(data);
        long filesSizeAfter;

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.closeAndCompact();
            Utils.assertEquals(dao.range(null, null), expectedData.entrySet());

            filesSizeAfter = getFilesSize(data);
            assertEquals(2, getFilesCount(data));
            assertTrue( filesSizeAfter < filesSizeBefore);
        }

        filesSizeAfter = getFilesSize(data);
        assertEquals(2, getFilesCount(data));
        assertTrue( filesSizeAfter < filesSizeBefore);
    }

    @Test
    void compactDuplicates(@TempDir Path data) throws IOException {
        TreeMap<ByteBuffer, ByteBuffer> expectedData = new TreeMap<>();
        long filesSizeBefore;
        long filesSizeAfter;
        for (int i = 0; i < 20; i++) {
            try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
                for (int j = 0; j < 20; j++) {
                    ByteBuffer key = wrap("key_" + j);
                    ByteBuffer value = wrap("value" + j);
                    dao.upsert(Record.of(key, value));
                    expectedData.put(key, value);
                }
            }
        }
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            filesSizeBefore = getFilesSize(data);
            dao.closeAndCompact();
            Utils.assertEquals(dao.range(null, null), expectedData.entrySet());

            filesSizeAfter = getFilesSize(data);
            assertEquals(2, getFilesCount(data));
            assertTrue( filesSizeAfter < filesSizeBefore);
        }

        filesSizeAfter = getFilesSize(data);
        assertEquals(2, getFilesCount(data));
        assertTrue( filesSizeAfter < filesSizeBefore);
    }

    @Test
    void compactRepeatable(@TempDir Path data) throws IOException {
        TreeMap<ByteBuffer, ByteBuffer> expectedData = new TreeMap<>();
        long filesSizeBefore;
        long filesSizeAfter;
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            for (int j = 0; j < 20; j++) {
                ByteBuffer key = wrap("key_" + j);
                ByteBuffer value = wrap("value" + j);
                dao.upsert(Record.of(key, value));
                expectedData.put(key, value);
            }

            dao.closeAndCompact();

            assertEquals(0, getFilesCount(data));
            Utils.assertEquals(dao.range(null, null), expectedData.entrySet());

            dao.closeAndCompact();

            assertEquals(0, getFilesCount(data));
            Utils.assertEquals(dao.range(null, null), expectedData.entrySet());
        }

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            filesSizeBefore = getFilesSize(data);
            Utils.assertEquals(dao.range(null, null), expectedData.entrySet());

            dao.closeAndCompact();

            filesSizeAfter = getFilesSize(data);
            assertEquals(2, getFilesCount(data));
            assertTrue( filesSizeAfter <= filesSizeBefore);
            Utils.assertEquals(dao.range(null, null), expectedData.entrySet());
        }
    }

    private int getDirSize(Path data) throws IOException {
        int[] size = new int[1];

        Files.walkFileTree(data, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                size[0] += (int) attrs.size();
                return FileVisitResult.CONTINUE;
            }
        });

        return size[0];
    }
}
