package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * Minimal database API.
 */
public interface DAO extends Closeable {
    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey);

    void upsert(Record record);

    /**
     * Appends {@code Byte.MIN_VALUE} to {@code buffer}.
     *
     * @param buffer original {@link ByteBuffer}
     * @return copy of {@code buffer} with {@code Byte.MIN_VALUE} appended
     */
    static ByteBuffer nextKey(ByteBuffer buffer) {
        ByteBuffer result = ByteBuffer.allocate(buffer.remaining() + 1);

        int position = buffer.position();

        result.put(buffer);
        result.put(Byte.MIN_VALUE);

        buffer.position(position);
        result.rewind();

        return result;
    }

    /**
     * Merge iterators.
     *
     * @param iterators list of iterators
     * @return merged iterator
     */
    static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        return new MergeIterator(iterators);
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

    class MergeIterator implements Iterator<Record> {
        private final PriorityQueue<QueueUnit> queue = new PriorityQueue<>();
        private final List<Iterator<Record>> iterators;
        private Record lastReturned;
        private int lastReturnedIndex;

        public MergeIterator(List<Iterator<Record>> iterators) {
            this.iterators = iterators;
            initQueue(iterators);
        }

        private void initQueue(List<Iterator<Record>> iterators) {
            for (int iterNumber = 0; iterNumber < iterators.size(); iterNumber++) {
                Iterator<Record> iterator = iterators.get(iterNumber);
                if (iterator.hasNext()) {
                    queue.add(new QueueUnit(iterator.next(), iterNumber));
                }
            }
        }

        @Override
        public boolean hasNext() {
            return queue.size() > 1;
        }

        @Override
        public Record next() {
            if (queue.isEmpty()) {
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

}
