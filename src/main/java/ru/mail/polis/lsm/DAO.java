package ru.mail.polis.lsm;

import ru.mail.polis.lsm.danilafedorov.LsmDAO;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;

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

    static SortedMap<ByteBuffer, Record> getSubMap(@Nullable final ByteBuffer fromKey, @Nullable final ByteBuffer toKey, SortedMap<ByteBuffer, Record> storage) {
        if (fromKey == null) {
            if (toKey == null) {
                return storage;
            }

            return storage.headMap(toKey);
        }

        if (toKey == null) {
            return storage.tailMap(fromKey);
        }

        return storage.subMap(fromKey, toKey);
    }

    /**
     * Merges iterators.
     *
     * @param iterators List
     * @return Iterator
     */
    static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        if (iterators.isEmpty()) {
            return Collections.emptyIterator();
        }
        if (iterators.size() == 1) {
            return iterators.get(0);
        }
        if (iterators.size() == 2) {
            return mergeTwo(iterators.get(0), iterators.get(1));
        }

        Iterator<Record> left = merge(iterators.subList(0, iterators.size() / 2));
        Iterator<Record> right = merge(iterators.subList(iterators.size() / 2, iterators.size()));
        return mergeTwo(left, right);
    }

    static Iterator<Record> mergeTwo(Iterator<Record> left, Iterator<Record> right) {
        return new LsmDAO.MergedRecordsIterator(left, right);
    }

    class MergedRecordsIterator implements Iterator<Record> {

        private final Iterator<Record> it1;
        private final Iterator<Record> it2;
        private Record next1;
        private Record next2;

        public MergedRecordsIterator(final Iterator<Record> left, final Iterator<Record> right) {
            it1 = right;
            it2 = left;
            getNext1();
            getNext2();
        }

        @Override
        public boolean hasNext() {
            return next1 != null || next2 != null;
        }

        @Override
        public Record next() {
            Record returnRecord = null;

            if (hasNext()) {
                if (next2 == null) {
                    returnRecord = next1;
                    getNext1();
                } else if (next1 == null) {
                    returnRecord = next2;
                    getNext2();
                } else {
                    int compareResult = next1.getKey().compareTo(next2.getKey());

                    if (compareResult <= 0) {
                        returnRecord = next1;
                        getNext1();

                        if (compareResult == 0) {
                            getNext2();
                        }
                    } else {
                        returnRecord = next2;
                        getNext2();
                    }
                }
            }

            return returnRecord;
        }

        private void getNext1() {
            next1 = it1.hasNext() ? it1.next() : null;
        }

        private void getNext2() {
            next2 = it2.hasNext() ? it2.next() : null;
        }
    }
}
