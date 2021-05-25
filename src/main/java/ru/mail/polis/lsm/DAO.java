package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Minimal database API.
 */
public interface DAO extends Closeable {

    PriorityQueue<Record> queue = new PriorityQueue<>(Comparator.comparing(Record::getKey));
    Set<Record> set = new TreeSet<>(Comparator.comparing(Record::getKey));

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


//        for (int i = 0; i < iterators.size(); i++) {
//
//            Iterator<Record> iterator = iterators.get(i);
//
//            while (iterator.hasNext()) {
//                set.add(iterator.next());
//            }
//
//        }
//
//        return set.iterator();
        return Collections.emptyIterator();
    }

}
