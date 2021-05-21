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

    static private Iterator<Record> mergeTwoIterators(Iterator<Record> left, Iterator<Record> right) {
        SortedMap<ByteBuffer, Record> records = new ConcurrentSkipListMap<>();
        Record leftRecord = null;
        boolean leftAdded = true;
        Record rightRecord = null;
        boolean rightAdded = true;
        while (left.hasNext() || right.hasNext() || !leftAdded || !rightAdded) {
            if (!left.hasNext() && leftAdded) {
                if (rightAdded) {
                    rightRecord = right.next();
                    records.put(rightRecord.getKey(), rightRecord);
                } else {
                    records.put(rightRecord.getKey(), rightRecord);
                    rightAdded = true;
                }
            } else if (!right.hasNext() && rightAdded) {
                if (leftAdded) {
                    leftRecord = left.next();
                    records.put(leftRecord.getKey(), leftRecord);
                } else {
                    records.put(leftRecord.getKey(), leftRecord);
                    leftAdded = true;
                }
            } else {
                if (leftAdded) {
                    leftRecord = left.next();
                    //noinspection UnusedAssignment
                    leftAdded = false;
                }
                if (rightAdded) {
                    rightRecord = right.next();
                    //noinspection UnusedAssignment
                    rightAdded = false;
                }
                int compareResult = leftRecord.getKey().compareTo(rightRecord.getKey());
                if (compareResult < 0) {
                    records.put(leftRecord.getKey(), leftRecord);
                    leftAdded = true;
                    rightAdded = false;
                } else if (compareResult > 0) {
                    records.put(rightRecord.getKey(), rightRecord);
                    leftAdded = false;
                    rightAdded = true;
                } else {
                    records.put(rightRecord.getKey(), rightRecord);
                    leftAdded = true;
                    rightAdded = true;
                }
            }
        }

        return records.values().iterator();
    }

}
