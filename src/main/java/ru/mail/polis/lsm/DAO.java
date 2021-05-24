package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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

    static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        if (iterators.isEmpty()) {
            return Collections.emptyIterator();
        }
        if (iterators.size() == 1) {
            return iterators.get(0);
        }
        if (iterators.size() == 2) {
            return mergeTwo(iterators.get(0), iterators.get(1));
        } else {
            Iterator<Record> left = merge(iterators.subList(0, iterators.size() / 2));
            Iterator<Record> right = merge(iterators.subList(iterators.size() / 2, iterators.size()));
            return mergeTwo(left, right);
        }
    }

    private static String toString(ByteBuffer buffer) {
        try {
            return StandardCharsets.UTF_8.newDecoder().decode(buffer).toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static Record updateRecord(Iterator<Record> iterator) {
        if (iterator.hasNext()) {
            return iterator.next();
        } else {
            return null;
        }
    }
    /**
     * Function that merge two iterators to one.
     */
    static Iterator<Record> mergeTwo(Iterator<Record> left, Iterator<Record> right) {

        if (!left.hasNext() && !right.hasNext()) {
            return Collections.emptyIterator();
        }
        if (left.hasNext() && !right.hasNext()) {
            return left;
        }
        if (!left.hasNext() && right.hasNext()) {
            return right;
        }

        final List<Record> listOfRecords = new ArrayList<>();
        Record leftRecord = updateRecord(left);
        Record rightRecord = updateRecord(right);

        while (!(leftRecord == null && rightRecord == null)) {

            if (leftRecord == null) {
                listOfRecords.add(rightRecord);
                rightRecord = updateRecord(right);
                if (rightRecord == null) {
                    return listOfRecords.iterator();
                }
                continue;
            }

            if (rightRecord == null) {
                listOfRecords.add(leftRecord);
                leftRecord = updateRecord(left);
                if (leftRecord == null) {
                    return listOfRecords.iterator();
                }
                continue;
            }

            int compare = toString(leftRecord.getKey()).compareTo(toString(rightRecord.getKey()));

            if (compare == 0) {
                listOfRecords.add(rightRecord);
                rightRecord = updateRecord(right);
                leftRecord = updateRecord(left);
            } else if (compare < 0) {
                listOfRecords.add(leftRecord);
                leftRecord = updateRecord(left);

            } else {
                listOfRecords.add(rightRecord);
                rightRecord = updateRecord(right);
            }
        }

        return listOfRecords.iterator();
    }

    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey);

    void upsert(Record record);

}
