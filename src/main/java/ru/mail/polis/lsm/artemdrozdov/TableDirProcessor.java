package ru.mail.polis.lsm.artemdrozdov;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static ru.mail.polis.lsm.artemdrozdov.SSTable.COMPACTION_FILE_NAME;
import static ru.mail.polis.lsm.artemdrozdov.SSTable.SSTABLE_FILE_PREFIX;
import static ru.mail.polis.lsm.artemdrozdov.SSTable.getIndexFile;

public final class TableDirProcessor {
    private TableDirProcessor() {
    }

    public static List<SSTable> loadFromDir(Path dir) throws IOException {
        Path compaction = dir.resolve(COMPACTION_FILE_NAME);
        if (Files.exists(compaction)) {
            try (Stream<Path> files = Files.list(dir)) {
                files.filter(file -> file.getFileName().startsWith(SSTABLE_FILE_PREFIX))
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        });
            }
            Path file0 = dir.resolve(SSTABLE_FILE_PREFIX + 0);
            if (Files.exists(getIndexFile(compaction))) {
                Files.move(getIndexFile(compaction), getIndexFile(file0), StandardCopyOption.ATOMIC_MOVE);
            }

            Files.move(compaction, file0, StandardCopyOption.ATOMIC_MOVE);
        }
        List<SSTable> result = new ArrayList<>();
        for (int i = 0; ; i++) {
            Path file = dir.resolve(SSTABLE_FILE_PREFIX + i);
            if (!Files.exists(file)) {
                return result;
            }
            result.add(new SSTable(file));
        }
    }
}
