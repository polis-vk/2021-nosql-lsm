package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;

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
        if (iterators.isEmpty()) {
            return Collections.emptyIterator();
        }
        if (iterators.size() == 1) {
            return iterators.get(0);
        }
        return new MergedIterator(iterators);
    }

    class MergedIterator implements Iterator<Record> {
        private final List<Iterator<Record>> iterators;

        private final ArrayList<Record> currentElements;
        private final Set<Integer> hasNextIndices = new TreeSet<>();
        private ByteBuffer lastNext;
        private int lastIteratorIndex;

        public MergedIterator(List<Iterator<Record>> iterators) {
            this.iterators = iterators;
            currentElements = new ArrayList<>();
            for (int i = 0; i < iterators.size(); ++i) {
                currentElements.add(null);
                Iterator<Record> currIter = iterators.get(i);
                if (currIter.hasNext()) {
                    Record currElement = currIter.next();
                    hasNextIndices.add(i);
                    currentElements.set(i, currElement);
                }
            }
        }

        @Override
        public boolean hasNext() {
            return !hasNextIndices.isEmpty();
        }

        @Override
        public Record next() {
            if (!hasNext()) throw new NoSuchElementException();

            int indexOfMinElement = -1;
            ByteBuffer currentMinKey = null;
            for (int i = 0; i < iterators.size(); i++) {
                Record currElement = currentElements.get(i);
                if (currElement != null &&
                        (currentMinKey == null || currElement.getKey().compareTo(currentMinKey) <= 0)) {
                    currentMinKey = currElement.getKey();
                    indexOfMinElement = i;
                }
            }

            Record minElement = currentElements.get(indexOfMinElement);
            Record nextRecord = Record.of(minElement.getKey(), minElement.getValue());
            lastNext = nextRecord.getKey();
            lastIteratorIndex = indexOfMinElement;

            Iterator<Record> currIter;
            Record currElement;

            for (int i = 0; i < iterators.size(); i++) {
                currElement = currentElements.get(i);
                if (currElement != null && currElement.getKey().equals(lastNext)) {
                    currIter = iterators.get(i);
                    while (currElement != null && currElement.getKey().equals(lastNext)) {
                        currElement = currIter.hasNext() ? currIter.next() : null;
                        if (i == lastIteratorIndex) break;
                    }
                    currentElements.set(i, currElement);
                    if (currElement == null) {
                        hasNextIndices.remove(i);
                    }
                }
            }
            return nextRecord;
        }
    }

}

