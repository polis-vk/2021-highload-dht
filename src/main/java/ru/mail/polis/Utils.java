package ru.mail.polis;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

public final class Utils {
    public Utils() {
        // Don't instantiate
    }

    public static Path getIndexFile(Path file) {
        return resolveWithExt(file, ".idx");
    }

    public static Path getTmpFile(Path file) {
        return resolveWithExt(file, ".tmp");
    }

    public static Path resolveWithExt(Path file, String ext) {
        return file.resolveSibling(file.getFileName() + ext);
    }

    public static MappedByteBuffer open(Path name) throws IOException {
        try (FileChannel channel = FileChannel.open(name, StandardOpenOption.READ)) {
            return channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        }
    }

    public static FileChannel openForWrite(Path tmpFile) throws IOException {
        return FileChannel.open(
                tmpFile,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
    }

    public static void rename(Path tmpFile, Path file) throws IOException {
        Files.deleteIfExists(file);
        Files.move(tmpFile, file, StandardCopyOption.ATOMIC_MOVE);
    }

    public static void writeValueWithSize(ByteBuffer value,
                                           WritableByteChannel channel,
                                           ByteBuffer size) throws IOException {
        writeInt(value.remaining(), channel, size);
        channel.write(size);
        channel.write(value);
    }

    public static void writeInt(int value,
                                 WritableByteChannel channel,
                                 ByteBuffer size) throws IOException {
        size.position(0);
        size.putInt(value);
        size.position(0);
        channel.write(size);
    }
}
