package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Minimal database API.
 */
public interface DAO extends Closeable {
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
     * @param iterators iterators List
     * @return result Iterator
     */
    static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        switch (iterators.size()) {
            case (0):
                return Collections.emptyIterator();
            case (1):
                return iterators.get(0);
            case (2):
                return new MergeTwo(iterators.get(0), iterators.get(1));
            default:
                Iterator<Record> left = merge(iterators.subList(0, iterators.size() / 2));
                Iterator<Record> right = merge(iterators.subList(iterators.size() / 2, iterators.size()));

                return new MergeTwo(left, right);
        }
    }

    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey);

    void upsert(Record record);

    class MergeTwo implements Iterator<Record> {
        private final Iterator<Record> left;
        private final Iterator<Record> right;
        private Record leftPart;
        private Record rightPart;

        private MergeTwo(Iterator<Record> left, Iterator<Record> right) {
            this.left = left;
            this.right = right;
            leftPart = left.next();
            rightPart = right.next();
        }

        private Record getNextOrNull(Iterator<Record> iterator) {
            if (iterator.hasNext()) {
                return iterator.next();
            } else return null;
        }

        @Override
        public boolean hasNext() {
            return leftPart != null || rightPart != null;
        }

        @Override
        public Record next() {
            if (leftPart == null && rightPart == null) {
                throw new NoSuchElementException();
            }

            Record result;

            if (leftPart == null) {
                result = rightPart;
                rightPart = getNextOrNull(right);
                return result;
            }

            if (rightPart == null) {
                result = leftPart;
                leftPart = getNextOrNull(left);
                return result;
            }

            int compareResult = leftPart.getKey().compareTo(rightPart.getKey());
            if (compareResult == 0) {
                result = rightPart;
                rightPart = getNextOrNull(right);
                leftPart = getNextOrNull(left);
            } else if (compareResult > 0) {
                result = rightPart;
                rightPart = getNextOrNull(right);
            } else {
                result = leftPart;
                leftPart = getNextOrNull(left);
            }

            return result;
        }
    }
}
