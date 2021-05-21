package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

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
        SortedMap<ByteBuffer, Record> records = new ConcurrentSkipListMap<>();
        Record leftRecord = left.next();
        Record rightRecord = right.next();
        while (leftRecord != null || rightRecord != null) {
            if (leftRecord == null) {
                records.put(rightRecord.getKey(), rightRecord);
                rightRecord = null;
                while (right.hasNext()) {
                    rightRecord = right.next();
                    records.put(rightRecord.getKey(), rightRecord);
                }
            } else if (rightRecord == null) {
                records.put(leftRecord.getKey(), leftRecord);
                leftRecord = null;
                while (left.hasNext()) {
                    leftRecord = left.next();
                    records.put(leftRecord.getKey(), leftRecord);
                }
            } else {
                int compareResult = leftRecord.getKey().compareTo(rightRecord.getKey());
                if (compareResult < 0) {
                    records.put(leftRecord.getKey(), leftRecord);
                    leftRecord = left.hasNext() ? left.next() : null;
                } else if (compareResult > 0) {
                    records.put(rightRecord.getKey(), rightRecord);
                    rightRecord = right.hasNext() ? right.next() : null;
                } else {
                    records.put(rightRecord.getKey(), rightRecord);
                    leftRecord = left.hasNext() ? left.next() : null;
                    rightRecord = right.hasNext() ? right.next() : null;
                }
            }
        }

        return records.values().iterator();
    }

}
