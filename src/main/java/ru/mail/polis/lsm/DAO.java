package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

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

        return new MergedRecordIterator<>(left, right);
    }

    class MergedRecordIterator<E extends Record> implements Iterator<E> {

        private final Iterator<E> left;
        private final Iterator<E> right;

        private E leftNext;
        private E rightNext;

        public MergedRecordIterator(Iterator<E> left, Iterator<E> right) {
            this.left = left;
            this.right = right;
            leftNext = left.hasNext() ? left.next() : null;
            rightNext = right.hasNext() ? right.next() : null;
        }

        @Override
        public boolean hasNext() {
            return leftNext != null || rightNext != null;
        }

        @Override
        public E next() {
            E toReturn;
            if (leftNext != null && rightNext != null) {
                int compareResult = leftNext.getKey().compareTo(rightNext.getKey());
                if (compareResult < 0) {
                    toReturn = leftNext;
                    leftNext = left.hasNext() ? left.next() : null;
                    return toReturn;
                } else if (compareResult > 0) {
                    toReturn = rightNext;
                    rightNext = right.hasNext() ? right.next() : null;
                    return toReturn;
                } else {
                    toReturn = rightNext;
                    leftNext = left.hasNext() ? left.next() : null;
                    rightNext = right.hasNext() ? right.next() : null;
                    return toReturn;
                }
            }
            if (leftNext == null) {
                toReturn = rightNext;
                rightNext = right.hasNext() ? right.next() : null;
            } else {
                toReturn = leftNext;
                leftNext = left.hasNext() ? left.next() : null;
            }
            return toReturn;
        }
    }

}
