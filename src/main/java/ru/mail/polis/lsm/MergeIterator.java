package ru.mail.polis.lsm;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

class MergeIterator implements Iterator<Record> {
    private final PriorityQueue<QueueUnit> queue = new PriorityQueue<>();
    private final List<Iterator<Record>> iterators;
    private Record lastReturned;
    private int lastReturnedIndex;

    public MergeIterator(List<Iterator<Record>> iterators) {
        this.iterators = iterators;
        initQueue();
    }

    private void initQueue() {
        for (int iterNumber = 0; iterNumber < iterators.size(); iterNumber++) {
            if (iterators.get(iterNumber).hasNext()) {
                queue.add(new QueueUnit(iterators.get(iterNumber).next(), iterNumber));
            }
        }
    }

    @Override
    public boolean hasNext() {
        synchronized (this) {
            return !queue.isEmpty() && (lastReturned == null
                    || queue.stream()
                    .anyMatch(queueUnit -> !queueUnit.getData().getKey().equals(lastReturned.getKey())));
        }
    }

    @Override
    public Record next() {
        synchronized (this) {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            Record result = null;

            while (!queue.isEmpty() && result == null) {
                QueueUnit current = queue.poll();

                Iterator<Record> currentIter = iterators.get(current.getSourceNumber());
                if (lastReturned == null || lastReturnedIndex == current.getSourceNumber()
                        || !current.getData().getKey().equals(lastReturned.getKey())) {
                    result = current.getData();
                    lastReturned = result;
                    lastReturnedIndex = current.getSourceNumber();
                }

                if (currentIter.hasNext()) {
                    queue.add(new QueueUnit(currentIter.next(), current.getSourceNumber()));
                }
            }
            return result;
        }
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
            int keyCompare = data.getKey().compareTo(o.getData().getKey());
            if (keyCompare == 0) {
                return o.getSourceNumber() - source;
            }
            return keyCompare;
        }
    }
}