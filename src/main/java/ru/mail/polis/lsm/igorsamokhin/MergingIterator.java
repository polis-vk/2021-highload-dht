package ru.mail.polis.lsm.igorsamokhin;

import ru.mail.polis.lsm.Record;

import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

public class MergingIterator implements Iterator<Record> {
    private final PriorityQueue<Entry> queue;

    MergingIterator(List<Iterator<Record>> iterators) {
        queue = new PriorityQueue<>((a, b) -> {
            int compare = a.prevRecord.compareKeyWith(b.prevRecord);
            if (compare == 0) {
                return a.order < b.order ? 1 : -1;
            }
            return compare;
        });

        for (int i = 0; i < iterators.size(); i++) {
            Iterator<Record> iterator = iterators.get(i);
            if (iterator.hasNext()) {
                queue.add(new Entry(iterator, iterator.next(), i));
            }
        }
    }

    @Override
    public boolean hasNext() {
        checkTombstones(queue);

        return !queue.isEmpty();
    }

    @Override
    public Record next() {
        checkTombstones(queue);

        Entry poll = queue.poll();
        if (poll == null) {
            return null;
        }
        clearQueue(queue, poll);
        Record record = poll.prevRecord;
        if (poll.iterator.hasNext()) {
            poll.prevRecord = poll.iterator.next();
            queue.add(poll);
        }

        return record;
    }

    /**
     * Skip all first tombstones.
     */
    private void checkTombstones(PriorityQueue<Entry> queue) {
        while (!queue.isEmpty() && (queue.peek().prevRecord.isTombstone())) {
            Entry head = queue.poll();

            clearQueue(queue, head);
            if (head.iterator.hasNext()) {
                head.prevRecord = head.iterator.next();
                queue.add(head);
            }
        }
    }

    /**
     * Delete first N elements of the queue, which are equals with given.
     */
    private void clearQueue(PriorityQueue<Entry> queue, Entry entry) {
        while (!queue.isEmpty() && (queue.peek().prevRecord.compareKeyWith(entry.prevRecord) == 0)) {
            Entry head = queue.poll();

            if (head != null && head.iterator.hasNext()) {
                head.prevRecord = head.iterator.next();
                queue.add(head);
            }
        }
    }
}
