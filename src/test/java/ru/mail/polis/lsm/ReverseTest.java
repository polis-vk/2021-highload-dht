package ru.mail.polis.lsm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.*;
import static ru.mail.polis.lsm.Utils.*;

public class ReverseTest {
    @Test
    void emptyReverse(@TempDir Path data) throws IOException {
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            ByteBuffer notExistedKey = ByteBuffer.wrap("NOT_EXISTED_KEY".getBytes(StandardCharsets.UTF_8));
            Iterator<Record> shouldBeEmptyReverse = dao.descendingRange(notExistedKey, null);
            assertFalse(shouldBeEmptyReverse.hasNext());
        }
    }

    @Test
    void middleScanReverse(@TempDir Path data) throws IOException {
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            Map<ByteBuffer, ByteBuffer> map = generateMap(0, 10);

            map.forEach((k, v) -> dao.upsert(Record.of(k, v)));

            Iterator<Record> descendingRange = dao.descendingRange(key(5), null);
            Utils.assertEquals(descendingRange, new TreeMap<>(generateMap(5, 10)).descendingMap().entrySet());
        }
    }

    @Test
    void rightScanReverse(@TempDir Path data) throws IOException {
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            Map<ByteBuffer, ByteBuffer> map = generateMap(0, 10);

            map.forEach((k, v) -> dao.upsert(Record.of(k, v)));
            Iterator<Record> descendingRange = dao.descendingRange(key(9), null);
            Utils.assertEquals(descendingRange, new TreeMap<>(generateMap(9, 10)).descendingMap().entrySet());
        }
    }

    @Test
    void removeAbsentReverse(@TempDir Path data) throws IOException {
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.upsert(Record.tombstone(wrap("NOT_EXISTED_KEY")));

            assertFalse(dao.descendingRange(null, null).hasNext());
        }
    }

    @Test
    void fsReverse(@TempDir Path data) throws IOException {
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.upsert(Record.of(key(1), value(1)));
        }

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            Iterator<Record> descendingRange = dao.descendingRange(null, null);
            assertTrue(descendingRange.hasNext());

            Record descRecord = descendingRange.next();
            assertEquals(key(1), descRecord.getKey());
            assertEquals(value(1), descRecord.getValue());
        }

        recursiveDelete(data);

        assertFalse(Files.exists(data));
        Files.createDirectory(data);

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            assertFalse(dao.descendingRange(null, null).hasNext());
        }
    }

    @Test
    void removeReverse(@TempDir Path data) throws IOException {
        // Reference value
        ByteBuffer key = wrap("SOME_KEY");
        ByteBuffer value = wrap("SOME_VALUE");

        // Create dao and fill data
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.upsert(Record.of(key, value));
            Iterator<Record> descendingRange = dao.descendingRange(null, null);

            assertTrue(descendingRange.hasNext());
            assertEquals(value, descendingRange.next().getValue());
        }

        // Load data and check
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            Iterator<Record> descendingRange = dao.descendingRange(null, null);

            assertTrue(descendingRange.hasNext());
            assertEquals(value, descendingRange.next().getValue());
            // Remove data and flush
            dao.upsert(Record.tombstone(key));
        }

        // Load and check not found
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            Iterator<Record> descendingRange = dao.descendingRange(null, null);

            assertFalse(descendingRange.hasNext());
        }
    }

    @Test
    void replaceWithCloseReverse(@TempDir Path data) throws Exception {
        ByteBuffer key = wrap("KEY");
        ByteBuffer value = wrap("VALUE_1");
        ByteBuffer value2 = wrap("VALUE_2");

        // Initial insert
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.upsert(Record.of(key, value));

            Iterator<Record> range = dao.descendingRange(null, null);
            assertTrue(range.hasNext());
            assertEquals(value, range.next().getValue());
        }

        // Reopen
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            Iterator<Record> range = dao.descendingRange(null, null);
            assertTrue(range.hasNext());
            assertEquals(value, range.next().getValue());

            // Replace
            dao.upsert(Record.of(key, value2));

            Iterator<Record> range2 = dao.descendingRange(null, null);
            assertTrue(range2.hasNext());
            assertEquals(value2, range2.next().getValue());
        }

        // Reopen
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            // Last value should win
            Iterator<Record> range2 = dao.descendingRange(null, null);
            assertTrue(range2.hasNext());
            assertEquals(value2, range2.next().getValue());
        }
    }

    @Test
    void burnReverse(@TempDir Path data) throws IOException {
        ByteBuffer key = wrap("FIXED_KEY");

        int overwrites = 100;
        for (int i = 0; i < overwrites; i++) {
            ByteBuffer value = value(i);
            try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
                dao.upsert(Record.of(key, value));
                assertEquals(value, dao.descendingRange(key, null).next().getValue());
            }

            // Check
            try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
                assertEquals(value, dao.descendingRange(key, null).next().getValue());
            }
        }
    }

    @Test
    void reversePersistenceTest(@TempDir Path data) throws IOException {
        TreeMap<ByteBuffer, ByteBuffer> source = new TreeMap<>();
        NavigableMap<ByteBuffer, ByteBuffer> result = source.descendingMap();
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            for (int i = 0; i < 10; i++) {
                Record record = Record.of(key(i), value(i));
                dao.upsert(record);
                source.put(record.getKey(), record.getValue());
            }

            Iterator<Record> descendingRange = dao.descendingRange(key(1), key(9));
            Utils.assertEquals(descendingRange, result.subMap(key(9), false, key(1), true).entrySet());

            descendingRange = dao.descendingRange(key(3), null);
            Utils.assertEquals(descendingRange, result.headMap(key(3), true).entrySet());

            descendingRange = dao.descendingRange(null, key(6));
            Utils.assertEquals(descendingRange, result.tailMap(key(6), false).entrySet());
        }

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            Iterator<Record> descendingRange = dao.descendingRange(key(1), key(9));
            Utils.assertEquals(descendingRange, result.subMap(key(9), false, key(1), true).entrySet());

            descendingRange = dao.descendingRange(key(3), null);
            Utils.assertEquals(descendingRange, result.headMap(key(3), true).entrySet());

            descendingRange = dao.descendingRange(null, key(6));
            Utils.assertEquals(descendingRange, result.tailMap(key(6), false).entrySet());
        }
    }

    @Test
    void compactEmptyReverse(@TempDir Path data) throws IOException {
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.closeAndCompact();
            Iterator<Record> descendingRange = dao.descendingRange(null, null);
            assertFalse(descendingRange.hasNext());
            assertEquals(0, getFilesCount(data));
        }
        assertEquals(0, getFilesCount(data));
    }


    @Test
    void hugeRecordsReverse(@TempDir Path data) throws IOException {
        // Reference value
        int size = 1024 * 1024;
        byte[] suffix = sizeBasedRandomData(size);
        int recordsCount = (int) (TestDaoWrapper.MAX_HEAP * 15 / size);

        prepareHugeDao(data, recordsCount, suffix);

        // Check
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            Iterator<Record> descendingRange = dao.descendingRange(null, null);

            for (int i = 0; i < recordsCount; i++) {
                verifyNext(suffix, descendingRange, recordsCount - i - 1);
            }

            assertFalse(descendingRange.hasNext());
        }
    }

    @Test
    void compactTombstonesReverse(@TempDir Path data) throws IOException {
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
            Utils.assertEquals(dao.descendingRange(null, null), expectedData.descendingMap().entrySet());

            filesSizeAfter = getFilesSize(data);
            assertEquals(2, getFilesCount(data));
            assertTrue( filesSizeAfter < filesSizeBefore);
        }

        filesSizeAfter = getFilesSize(data);
        assertEquals(2, getFilesCount(data));
        assertTrue( filesSizeAfter < filesSizeBefore);
    }

    @Test
    void compactDuplicatesReverse(@TempDir Path data) throws IOException {
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
            Utils.assertEquals(dao.descendingRange(null, null), expectedData.descendingMap().entrySet());

            filesSizeAfter = getFilesSize(data);
            assertEquals(2, getFilesCount(data));
            assertTrue( filesSizeAfter < filesSizeBefore);
        }

        filesSizeAfter = getFilesSize(data);
        assertEquals(2, getFilesCount(data));
        assertTrue( filesSizeAfter < filesSizeBefore);
    }

    @Test
    void compactRepeatableReverse(@TempDir Path data) throws IOException {
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
            Utils.assertEquals(dao.descendingRange(null, null), expectedData.descendingMap().entrySet());

            dao.closeAndCompact();

            assertEquals(0, getFilesCount(data));
            Utils.assertEquals(dao.descendingRange(null, null), expectedData.descendingMap().entrySet());
        }

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            filesSizeBefore = getFilesSize(data);
            Utils.assertEquals(dao.descendingRange(null, null), expectedData.descendingMap().entrySet());

            dao.closeAndCompact();

            filesSizeAfter = getFilesSize(data);
            assertEquals(2, getFilesCount(data));
            assertTrue( filesSizeAfter <= filesSizeBefore);
            Utils.assertEquals(dao.descendingRange(null, null), expectedData.descendingMap().entrySet());
        }
    }
}
