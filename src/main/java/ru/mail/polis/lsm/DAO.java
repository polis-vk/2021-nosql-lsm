package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.*;

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

    private static void addLast(Record leftRecord, Record rightRecord,
                                ArrayList<Record> list) {
        int comparisonResult = leftRecord.getKey().compareTo(rightRecord.getKey());
        if (comparisonResult < 0) {
            list.add(leftRecord);
            list.add(rightRecord);
        } else if (comparisonResult > 0) {
            list.add(rightRecord);
            list.add(leftRecord);
        } else {
            list.add(rightRecord);
        }
    }

    static Iterator<Record> mergeTwo(Iterator<Record> left, Iterator<Record> right) {
        ArrayList<Record> list = new ArrayList<>();

        Record leftRecord = left.next();
        Record rightRecord = right.next();

        int comparisonResult;

        while (left.hasNext() && right.hasNext()) {
            comparisonResult = leftRecord.getKey().compareTo(rightRecord.getKey());
            if (comparisonResult < 0) {
                list.add(leftRecord);
                leftRecord = left.next();
            } else if (comparisonResult > 0) {
                list.add(rightRecord);
                rightRecord = right.next();
            } else {
                list.add(rightRecord);
                leftRecord = left.next();
                rightRecord = right.next();
            }
        }
        if (!left.hasNext()) {
            boolean leftWasUsed = false;
            while (right.hasNext()) {
                comparisonResult = leftRecord.getKey().compareTo(rightRecord.getKey());
                if (comparisonResult < 0) {
                    list.add(leftRecord);
                    leftWasUsed = true;
                    break;
                } else if (comparisonResult > 0) {
                    list.add(rightRecord);
                    rightRecord = right.next();
                } else {
                    list.add(rightRecord);
                    rightRecord = right.next();
                    leftWasUsed = true;
                    break;
                }
            }
            if (leftWasUsed) {
                while (right.hasNext()) {
                    list.add(rightRecord);
                    rightRecord = right.next();
                }
                list.add(rightRecord);
            } else {
                addLast(leftRecord, rightRecord, list);
            }
        } else {
            boolean rightWasUsed = false;
            while (left.hasNext()) {
                comparisonResult = leftRecord.getKey().compareTo(rightRecord.getKey());
                if (comparisonResult < 0) {
                    list.add(leftRecord);
                    leftRecord = left.next();
                } else if (comparisonResult > 0) {
                    list.add(rightRecord);
                    rightWasUsed = true;
                    break;
                } else {
                    list.add(rightRecord);
                    rightWasUsed = true;
                    break;
                }
            }
            if (rightWasUsed) {
                while (left.hasNext()) {
                    list.add(leftRecord);
                    leftRecord = left.next();
                }
                list.add(leftRecord);
            } else {
                addLast(leftRecord, rightRecord, list);
            }
        }

        return list.iterator();
    }

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
}
