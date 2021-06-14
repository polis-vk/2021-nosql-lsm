package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Minimal database API.
 */
public interface DAO extends Closeable {
    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey);

    void upsert(Record record) throws UncheckedIOException;

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
     * Merges iterators.
     *
     * @param iterators iterators to merge
     * @return merged iterator
     */
    static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        switch (iterators.size()) {
            case 0:
                return Collections.emptyIterator();
            case 1:
                return iterators.get(0);
            case 2:
                return mergeTwoIterators(iterators.get(0), iterators.get(1));
            default:
                Iterator<Record> left = merge(iterators.subList(0, iterators.size() / 2));
                Iterator<Record> right = merge(iterators.subList(iterators.size() / 2, iterators.size()));
                return mergeTwoIterators(left, right);
        }
    }

    /**
     * Merges two iterators together.
     *
     * @param left the first iterator
     * @param right the second iterator
     * @return merged iterator
     */
    static Iterator<Record> mergeTwoIterators(Iterator<Record> left, Iterator<Record> right) {
        if (!left.hasNext()) {
            return right;
        }
        if (!right.hasNext()) {
            return left;
        }

        return new MergedRecordIterator(left, right);
    }

    class MergedRecordIterator implements Iterator<Record> {

        private final Iterator<Record> left;
        private final Iterator<Record> right;

        private Record leftNext;
        private Record rightNext;

        public MergedRecordIterator(Iterator<Record> left, Iterator<Record> right) {
            this.left = left;
            this.right = right;
            leftNext = getNextRecord(left);
            while (leftNext != null && leftNext.isTombstone()) {
                leftNext = getNextRecord(left);
            }
            rightNext = getNextRecord(right);
            while (rightNext != null && rightNext.isTombstone()) {
                rightNext = getNextRecord(right);
            }
        }

        @Override
        public boolean hasNext() {
            return leftNext != null || rightNext != null;
        }

        @Override
        public Record next() {
            Record toReturn = getRecordToReturn();
            while (toReturn != null && toReturn.isTombstone()) {
                toReturn = getRecordToReturn();
            }
            return toReturn;
        }

        private Record getRecordToReturn() {
            Record toReturn;
            if (leftNext != null && rightNext != null) {
                int compareResult = leftNext.getKey().compareTo(rightNext.getKey());
                if (compareResult < 0) {
                    toReturn = leftNext;
                    leftNext = getNextRecord(left);
                    return toReturn;
                } else if (compareResult > 0) {
                    toReturn = rightNext;
                    rightNext = getNextRecord(right);
                    return toReturn;
                } else {
                    toReturn = rightNext;
                    leftNext = getNextRecord(left);
                    rightNext = getNextRecord(right);
                    return toReturn;
                }
            }
            if (leftNext == null) {
                toReturn = rightNext;
                rightNext = getNextRecord(right);
            } else {
                toReturn = leftNext;
                leftNext = getNextRecord(left);
            }
            return toReturn;
        }

        private Record getNextRecord(Iterator<Record> iterator) {
            return iterator.hasNext() ? iterator.next() : null;
        }
    }

}
