package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Minimal database API.
 */
public interface DAO extends Closeable {
    //вернет диапазон(ключ может быть null -> с начала/до конча)
    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey);

    //добавить или обновить значение
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

    static Iterator<Record> mergeTwoIterators(Iterator<Record> left, Iterator<Record> right) {

        return new Iterator<>() {

            Record leftRecord;
            Record rightRecord;

            @Override
            public boolean hasNext() {
                return left.hasNext() || right.hasNext();
            }

            @Override
            public Record next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("No elements");
                }

                leftRecord = getNext(left, leftRecord);
                rightRecord = getNext(right, rightRecord);

                if (leftRecord == null) {
                    return consumeRight();
                }
                if (rightRecord == null) {
                    return consumeLeft();
                }
                int comp = leftRecord.getKey().compareTo(rightRecord.getKey());
                if (comp == 0) {
                    leftRecord = null;
                    return consumeRight();
                }
                if (comp < 0) {
                    return consumeLeft();
                } else {
                    return consumeRight();
                }
            }

            private Record consumeLeft() {
                Record tmp = this.leftRecord;
                leftRecord = null;
                return tmp;
            }

            private Record consumeRight() {
                Record tmp = this.rightRecord;
                rightRecord = null;
                return tmp;
            }


            private Record getNext(Iterator<Record> iterator, @Nullable  Record current) {
                if (current != null) {
                    return current;
                }
                if (iterator.hasNext()) {
                    return iterator.next();
                }
                return null;
            }
        };
    }


    static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        if (iterators.size() == 0) {
            return Collections.emptyIterator();
        }
        if (iterators.size() == 1) {
            return iterators.get(0);
        }
        if (iterators.size() == 2) {
            return mergeTwoIterators(iterators.get(0), iterators.get(1));
        }
        Iterator<Record> left = merge(iterators.subList(0, iterators.size() / 2));
        Iterator<Record> right = merge(iterators.subList(iterators.size() / 2, iterators.size()));
        return mergeTwoIterators(left, right);
    }
}