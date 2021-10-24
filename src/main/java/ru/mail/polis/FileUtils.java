/*
 * Copyright 2020 (c) Odnoklassniki
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

package ru.mail.polis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Utility methods for handling files.
 *
 * @author Vadim Tsesko
 */
public final class FileUtils {
    private static final String TEMP_PREFIX = "highload-dht";

    private static final Logger LOG = LoggerFactory.getLogger(FileUtils.class);

    private FileUtils() {
        // Don't instantiate
    }

    /**
     * Creates and returns a new unused yet temporary directory.
     */
    public static Path createTempDirectory() throws IOException {
        final Path data = Files.createTempDirectory(TEMP_PREFIX);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (Files.exists(data)) {
                    recursiveDelete(data);
                }
            } catch (IOException e) {
                LOG.error("Can't delete temporary directory: {}", data, e);
            }
        }));
        return data;
    }

    /**
     * Recursively removes path.
     */
    public static void recursiveDelete(final Path path) throws IOException {
        Files.walkFileTree(
                path,
                new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(
                            final Path file,
                            final BasicFileAttributes attrs) throws IOException {
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(
                            final Path dir,
                            final IOException exc) throws IOException {
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }
                });
    }

    public static FileChannel openFileChannel(Path path) throws IOException {
        return FileChannel.open(
                path,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
    }

    public static void clean(MappedByteBuffer mappedByteBuffer) throws IOException {
        try {
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            Object unsafe = unsafeField.get(null);
            Method invokeCleaner = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
            invokeCleaner.invoke(unsafe, mappedByteBuffer);
        } catch (ClassNotFoundException | NoSuchFieldException | NoSuchMethodException
                | IllegalAccessException | InvocationTargetException e) {
            throw new IOException(e);
        }
    }

    public static void writeSizeAndValue(
            ByteBuffer value,
            WritableByteChannel channel,
            ByteBuffer tmp
    ) throws IOException {
        tmp.position(0);
        tmp.putInt(value.remaining());
        tmp.position(0);

        channel.write(tmp);
        channel.write(value);
    }

    public static void writeInt(WritableByteChannel fileChannel, ByteBuffer tmp, int value) throws IOException {
        tmp.position(0);
        tmp.putInt(value);
        tmp.position(0);

        fileChannel.write(tmp);
    }
}
