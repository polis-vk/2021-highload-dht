package ru.mail.polis.lsm;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

class MergeIterator implements Iterator<Record> {
    private final PriorityQueue<QueueUnit> queue = new PriorityQueue<>();
    private final List<Iterator<Record>> iterators;
    private final boolean isDirectOrder;
    private Record current;
    private Record lastReturned;
    private int lastReturnedIndex;

    public MergeIterator(List<Iterator<Record>> iterators, boolean isDirectOrder) {
        this.iterators = iterators;
        this.isDirectOrder = isDirectOrder;
        initQueue();
    }

    private void initQueue() {
        for (int iterNumber = 0; iterNumber < iterators.size(); iterNumber++) {
            if (iterators.get(iterNumber).hasNext()) {
                queue.add(new QueueUnit(iterators.get(iterNumber).next(), iterNumber));
            }
        }
        getCurrent();
    }

    private void getCurrent() {
        Record result = null;

        while (!queue.isEmpty() && result == null) {
            QueueUnit currentElem = queue.poll();

            Iterator<Record> currentIter = iterators.get(currentElem.getSourceNumber());
            if (lastReturned == null || lastReturnedIndex == currentElem.getSourceNumber()
                    || !currentElem.getData().getKey().equals(lastReturned.getKey())) {
                result = currentElem.getData();
                lastReturned = result;
                lastReturnedIndex = currentElem.getSourceNumber();
            }

            if (currentIter.hasNext()) {
                queue.add(new QueueUnit(currentIter.next(), currentElem.getSourceNumber()));
            }
        }
        current = result;
    }

    @Override
    public boolean hasNext() {
        synchronized (this) {
            return current != null;
        }
    }

    @Override
    public Record next() {
        if (!hasNext()) throw new NoSuchElementException();
        Record result = current;
        getCurrent();
        return result;
    }

    class QueueUnit implements Comparable<QueueUnit> {
        private final Record data;
        private final int source;

        public QueueUnit(Record data, int source) {
            this.data = data;
            this.source = source;
        }

        public Record getData() {
            return data;
        }

        public int getSourceNumber() {
            return source;
        }

        @Override
        public int compareTo(QueueUnit o) {
            int keyCompare;
            if (isDirectOrder) {
                keyCompare = data.getKey().compareTo(o.getData().getKey());
            } else {
                keyCompare = o.getData().getKey().compareTo(data.getKey());
            }

            if (keyCompare == 0) {
                return o.getSourceNumber() - source;
            }
            return keyCompare;
        }
    }
}
