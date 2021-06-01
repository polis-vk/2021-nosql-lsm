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

    static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        switch (iterators.size()) {
            case (0):
                return Collections.emptyIterator();
            case (1):
                return iterators.get(0);
            case (2):
                return mergeTwo(iterators.get(0), iterators.get(1));
        }

        Iterator<Record> left = merge(iterators.subList(0, iterators.size() / 2));
        Iterator<Record> right = merge(iterators.subList(iterators.size() / 2, iterators.size()));

        if (left.hasNext() && !right.hasNext()) {
            return left;
        } else if (!left.hasNext() && right.hasNext()) {
            return right;
        } else return mergeTwo(left, right);
    }

    static Iterator<Record> mergeTwo(Iterator<Record> left, Iterator<Record> right) {
        final ArrayList<Record> result = new ArrayList<>();

        Record leftPart = left.next();
        Record rightPart = right.next();
        int compareParts;

        while (leftPart != null || rightPart != null) {
            if (leftPart == null) {
                result.add(rightPart);
                if (right.hasNext()) {
                    rightPart = right.next();
                } else {
                    rightPart = null;
                }
            } else if (rightPart == null) {
                result.add(leftPart);
                if (left.hasNext()) {
                    leftPart = left.next();
                } else {
                    leftPart = null;
                }
            }
            if (leftPart != null && rightPart != null) {
                compareParts = leftPart.getKey().compareTo(rightPart.getKey());
                if (compareParts == 0) {
                    result.add(rightPart);
                    rightPart = checkNext(right);
                    leftPart = checkNext(left);
                } else if (compareParts > 0) {
                    result.add(rightPart);
                    rightPart = checkNext(right);
                } else {
                    result.add(leftPart);
                    leftPart = checkNext(left);
                }
            }
        }
        return result.iterator();
    }

    private static Record checkNext(Iterator<Record> iterator) {
        if (iterator.hasNext()) {
            return iterator.next();
        } else return null;
    }

}
