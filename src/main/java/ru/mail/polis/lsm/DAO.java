package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
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

    /**
     * Method merges iterator list.
     * @param iterators - iterators list
     * @return - merged iterator
     */

    static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        SortedMap<ByteBuffer, Record> resultMap = new ConcurrentSkipListMap<>(ByteBuffer::compareTo);
        for (Iterator<Record> iterator : iterators) {
            while (iterator.hasNext()) {
                Record currentRecord = iterator.next();
                resultMap.put(currentRecord.getKey(), currentRecord);
            }
        }
        return resultMap.values().iterator();
    }
}
