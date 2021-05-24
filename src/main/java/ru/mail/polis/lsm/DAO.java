package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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


    static Iterator<Record> mergeTwo(Iterator<Record> list1, Iterator<Record> list2) {
        final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
        Record one = list1.hasNext() ? list1.next() : null;
        Record two = list2.hasNext() ? list2.next() : null;
        while (one != null || two != null) {
            if (one == null) {
                storage.put(two.getKey(), Record.of(two.getKey(), two.getValue()));
                if (list2.hasNext())
                    two = list2.next();
                else
                    two = null;
                continue;
            }
            if (two == null) {
                storage.put(one.getKey(), Record.of(one.getKey(), one.getValue()));
                if (list1.hasNext())
                    one = list1.next();
                else
                    one = null;
                continue;
            }
            if (one.getKey().compareTo(two.getKey()) > 0) {
                storage.put(two.getKey(), Record.of(two.getKey(), two.getValue()));
                if (list2.hasNext())
                    two = list2.next();
                else
                    two = null;
            } else {
                storage.put(one.getKey(), Record.of(one.getKey(), one.getValue()));
                if (list1.hasNext())
                    one = list1.next();
                else
                    one = null;
            }
        }
        return storage.values().iterator();
    }

    static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        if (iterators.size() == 0) {
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

}
