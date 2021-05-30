package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
     * Method that merge iterators and return iterator.
     *
     * @param iterators is list of iterators to merge
     * @return merged iterators
     */
    static Iterator<Record> merge(List<Iterator<Record>> iterators) {

        Map<ByteBuffer, Record> map = new TreeMap<>(ByteBuffer::compareTo);

        for (Iterator<Record> iterator : iterators) {
            while (iterator.hasNext()) {
                Record record = iterator.next();
                map.put(record.getKey(), record);
            }
        }

        return map.values().iterator();
    }

}
