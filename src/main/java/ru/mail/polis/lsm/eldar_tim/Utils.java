package ru.mail.polis.lsm.eldar_tim;

import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.eldar_tim.components.MemTable;
import ru.mail.polis.lsm.eldar_tim.components.SSTable;
import ru.mail.polis.lsm.eldar_tim.iterators.MergeIterator;
import ru.mail.polis.lsm.eldar_tim.iterators.PeekingIterator;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;

public final class Utils {

    private Utils() {
        // Don't instantiate
    }

    public static Iterator<Record> sstableRanges(
            List<SSTable> tables, @Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey
    ) {
        List<Iterator<Record>> iterators = new ArrayList<>(tables.size());
        for (SSTable ssTable : tables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return merge(iterators);
    }

    public static SortedMap<ByteBuffer, Record> map(
            MemTable memTable, @Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey
    ) {
        if (fromKey == null && toKey == null) {
            return memTable.raw();
        }
        if (fromKey == null) {
            return memTable.raw().headMap(toKey);
        }
        if (toKey == null) {
            return memTable.raw().tailMap(fromKey);
        }
        return memTable.raw().subMap(fromKey, toKey);
    }

    public static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        if (iterators.isEmpty()) {
            return Collections.emptyIterator();
        }
        if (iterators.size() == 1) {
            return iterators.get(0);
        }
        if (iterators.size() == 2) {
            return mergeTwo(iterators.get(0), iterators.get(1));
        }
        Iterator<Record> left = merge(iterators.subList(0, iterators.size() / 2));
        Iterator<Record> right = merge(iterators.subList(iterators.size() / 2, iterators.size()));
        return mergeTwo(left, right);
    }

    public static Iterator<Record> mergeTwo(Iterator<Record> left, Iterator<Record> right) {
        return new MergeIterator(new PeekingIterator(left), new PeekingIterator(right));
    }
}
