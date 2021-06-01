package ru.mail.polis.lsm;

import javax.annotation.Nonnull;
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
     * @param iterators List
     * @return Iterator
     */
    static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        return MergedRecordsIterator.instanceOf(iterators);
    }

    class MergedRecordsIterator implements Iterator<Record> {
        private final Iterator<Record> firstIter;
        private final Iterator<Record> secondIter;
        private Record firstRecord;
        private Record secondRecord;

        public static Iterator<Record> instanceOf(List<Iterator<Record>> iterators) {
            if (iterators.isEmpty()) {
                return Collections.emptyIterator();
            }

            var size = iterators.size();
            if (size == 1) {
                return iterators.get(0);
            }

            return merge(
                    instanceOf(iterators.subList(0, size / 2)),
                    instanceOf(iterators.subList(size / 2, iterators.size()))
            );
        }

        private static Iterator<Record> merge(Iterator<Record> left, Iterator<Record> right) {
            return new MergedRecordsIterator(left, right);
        }

        public MergedRecordsIterator(final Iterator<Record> left, final Iterator<Record> right) {
            firstIter = right;
            secondIter = left;

            this.firstRecord = getElement(firstIter);
            this.secondRecord = getElement(secondIter);
        }

        @Override
        public boolean hasNext() {
            return firstRecord != null || secondRecord != null;
        }

        @Override
        public Record next() {
            if (!hasNext()) {
                throw new NullPointerException("No Such Element");
            }

            final var compareResult = compare(firstRecord, secondRecord);
            final var next = compareResult > 0
                    ? secondRecord
                    : firstRecord;

            if (compareResult < 0) {
                firstRecord = getElement(firstIter);
            }

            if (compareResult > 0) {
                secondRecord = getElement(secondIter);
            }

            if (compareResult == 0) {
                firstRecord = getElement(firstIter);
                secondRecord = getElement(secondIter);
            }

            return next;
        }

        private int compare(@Nullable Record r1, @Nullable Record r2) {
            boolean firstNull = r1 == null;
            boolean secondNull = r2 == null;

            if (firstNull) {
                return 1;
            }

            if (secondNull) {
                return -1;
            }

            return r1.getKey().compareTo(r2.getKey());
        }

        private Record getElement(@Nonnull final Iterator<Record> iter) {
            return iter.hasNext() ? iter.next() : null;
        }
    }

    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey);

    void upsert(Record record);
}
