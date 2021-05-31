package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.stream.IntStream;

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
     * Merge Iterators.
     *
     * @param iterators List
     * @return Iterator
     */
    static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        return new MergeRecordIterator(iterators);
    }

    /**
     * MergeIterator.
     */
    class MergeRecordIterator implements Iterator<Record> {

        private final Queue<PeekRecordIterator> queue = new PriorityQueue<>();

        public MergeRecordIterator(List<Iterator<Record>> iterators) {
            IntStream
                .range(0, iterators.size())
                .filter(i -> iterators.get(i).hasNext())
                .forEach(i -> queue.offer(new PeekRecordIterator(iterators.get(i), i)));
        }

        @Override
        public boolean hasNext() {
            return !queue.isEmpty();
        }

        @Override
        public Record next() {
            PeekRecordIterator peekIt = queue.remove();
            PeekRecordIterator peekItFind = queue.poll();

            while (peekItFind != null) {
                ByteBuffer currentKey = peekIt.peek().getKey();
                ByteBuffer findKey = peekItFind.peek().getKey();

                if (!currentKey.equals(findKey)) break;

                if (peekItFind.hasDoubleNext()) {
                    peekItFind.next();
                    queue.offer(peekItFind);
                }
                peekItFind = queue.poll();
            }
            if (peekItFind != null) queue.offer(peekItFind);

            Record returnNext = peekIt.next();
            if (peekIt.hasNext()) queue.offer(peekIt);
            return returnNext;
        }
    }

    /**
     * PeekIterator.
     */
    class PeekRecordIterator implements Iterator<Record>, Comparable<PeekRecordIterator> {

        private final Iterator<Record> iterator;
        private final Integer number;

        private boolean isPeek;
        private Record peekRecord;

        public PeekRecordIterator(Iterator<Record> iterator, Integer number) {
            this.iterator = iterator;
            this.number = number;
        }

        public Record peek() {
            if (!isPeek) {
                peekRecord = iterator.next();
                isPeek = true;
            }
            return peekRecord;
        }

        public Integer getNumber() {
            return number;
        }

        public boolean hasDoubleNext() {
            return isPeek ? iterator.hasNext() : hasNext();
        }

        @Override
        public boolean hasNext() {
            return isPeek || iterator.hasNext();
        }

        @Override
        public Record next() {
            if (!isPeek) return iterator.next();

            Record next = peekRecord;
            peekRecord = null;
            isPeek = false;
            return next;
        }

        @Override
        public int compareTo(PeekRecordIterator o) {
            int compareKeyASC = this.peek().getKey().compareTo(o.peek().getKey());
            int compareNumberDESC = o.getNumber().compareTo(this.getNumber());
            return compareKeyASC == 0 ? compareNumberDESC : compareKeyASC;
        }
    }
}
