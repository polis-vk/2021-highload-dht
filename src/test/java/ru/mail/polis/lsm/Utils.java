package ru.mail.polis.lsm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

class Utils {

    private static final String KEY_PREFIX = "KEY_";
    private static final String VALUE_PREFIX = "VALUE_";

    static Map<ByteBuffer, ByteBuffer> generateMap(int from, int to) {
        Map<ByteBuffer, ByteBuffer> map = new HashMap<>();
        for (int i = from; i < to; i++) {
            map.put(key(i), value(i));
        }
        return map;
    }

    static Map<ByteBuffer, ByteBuffer> mapOf(String... keyValue) {
        Map<ByteBuffer, ByteBuffer> map = new HashMap<>(keyValue.length / 2);
        for (int i = 0; i < keyValue.length; i += 2) {
            map.put(wrap(keyValue[i]), wrap(keyValue[i + 1]));
        }
        return map;
    }

    static ByteBuffer wrap(String text) {
        return ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
    }

    static ByteBuffer key(int index) {
        return wrap(KEY_PREFIX + index);
    }

    static byte[] sizeBasedRandomData(int size) {
        Random random = new Random(size);
        byte[] result = new byte[size];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) ('A' + random.nextInt(26));
        }
        return result;
    }

    static ByteBuffer keyWithSuffix(int key, byte[] suffix) {
        String binary = Long.toBinaryString(key);
        int leadingN = 64 - binary.length();
        String builder = "0".repeat(leadingN) + binary;

        byte[] keyBytes = (KEY_PREFIX + "_" + builder).getBytes(StandardCharsets.UTF_8);
        byte[] result = new byte[keyBytes.length + suffix.length];
        System.arraycopy(keyBytes, 0, result, 0, keyBytes.length);
        System.arraycopy(suffix, 0, result, keyBytes.length, suffix.length);
        return ByteBuffer.wrap(result);
    }

    static ByteBuffer valueWithSuffix(int value, byte[] suffix) {
        byte[] valueBytes = (VALUE_PREFIX + "_" + value).getBytes(StandardCharsets.UTF_8);
        return join(valueBytes, suffix);
    }

    static ByteBuffer join(byte[] a, byte[] b) {
        byte[] result = new byte[a.length + b.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return ByteBuffer.wrap(result);
    }

    static ByteBuffer value(int index) {
        return wrap(VALUE_PREFIX + index);
    }

    static void assertDaoEquals(DAO dao, Map<ByteBuffer, ByteBuffer> map) {
        TreeMap<ByteBuffer, ByteBuffer> bufferTreeMap = new TreeMap<>(map);

        assertEquals(dao.range(null, null), bufferTreeMap.entrySet());

        if (!bufferTreeMap.isEmpty()) {
            assertEquals(dao.range(bufferTreeMap.firstKey(), DAO.nextKey(bufferTreeMap.lastKey())), bufferTreeMap.entrySet());
            assertEquals(dao.range(bufferTreeMap.firstKey(), null), bufferTreeMap.entrySet());
            assertEquals(dao.range(null, DAO.nextKey(bufferTreeMap.lastKey())), bufferTreeMap.entrySet());
        }
    }

    // THIS METHOD IS FOR TESTS ONLY - IT IS EXTREMELY NOT EFFECTIVE
    static void assertEquals(Iterator<Record> i1, Collection<Map.Entry<ByteBuffer, ByteBuffer>> i2) {
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();

        List<String> i1List = new ArrayList<>();
        i1.forEachRemaining(r -> i1List.add(toString(decoder, r.getKey(), r.getValue())));

        List<String> i2List = new ArrayList<>();
        for (Map.Entry<ByteBuffer, ByteBuffer> entry : i2) {
            i2List.add(toString(decoder, entry.getKey(), entry.getValue()));
        }

        assertIterableEquals(i2List, i1List);
    }

    public static String toString(ByteBuffer buffer) {
        try {
            return StandardCharsets.UTF_8.newDecoder().decode(buffer).toString();
        } catch (CharacterCodingException e) {
            throw new RuntimeException(e);
        }
    }

    private static String toString(CharsetDecoder decoder, ByteBuffer key, ByteBuffer value) {
        try {
            return decoder.decode(key.duplicate()) + ": " + decoder.decode(value.duplicate());
        } catch (CharacterCodingException e) {
            throw new RuntimeException(e);
        }
    }

    static void recursiveDelete(Path path) throws IOException {
        Files.walkFileTree(path,
                new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }
                });
    }
}
