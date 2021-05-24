package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
     * Merges list of sorted iterators.
     *
     * @param iterators is list of sorted iterators
     * @return iterator over sorted records
     */
    static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        int size = iterators.size();
        if (size == 0) {
            return Collections.emptyIterator();
        } else if (size == 1) {
            return iterators.get(0);
        } else if (size == 2) {
            return mergeTwo(iterators.get(0), iterators.get(1));
        } else {
            int middle = size / 2;
            Iterator<Record> left = merge(iterators.subList(0, middle));
            Iterator<Record> right = merge(iterators.subList(middle, size));
            return mergeTwo(left, right);
        }
    }

    /**
     * Merges two sorted iterators.
     *
     * @param left has lower priority
     * @param right has higher priority
     * @return iterator over merged sorted records
     */
    static Iterator<Record> mergeTwo(Iterator<Record> left, Iterator<Record> right) {
        List<Record> list = new ArrayList<>();

        Record leftRecord = nextOf(left);
        Record rightRecord = nextOf(right);

        int comparisonResult;

        while (leftRecord != null && rightRecord != null) {
            comparisonResult = leftRecord.getKey().compareTo(rightRecord.getKey());
            if (comparisonResult < 0) {
                list.add(leftRecord);
                leftRecord = nextOf(left);
            } else if (comparisonResult > 0) {
                list.add(rightRecord);
                rightRecord = nextOf(right);
            } else {
                list.add(rightRecord);
                leftRecord = nextOf(left);
                rightRecord = nextOf(right);
            }
        }
        while (leftRecord != null) {
            list.add(leftRecord);
            leftRecord = nextOf(left);
        }
        while (rightRecord != null) {
            list.add(rightRecord);
            rightRecord = nextOf(right);
        }

        return list.iterator();
    }

    /**
     * Gets next record if it exists.
     *
     * @param iterator is an Iterator over sorted records
     * @return next record or null
     */
    private static Record nextOf(Iterator<Record> iterator) {
        if (iterator.hasNext()) {
            return iterator.next();
        } else {
            return null;
        }
    }
}
