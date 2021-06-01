package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

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

    /**
     * Merges iterators without only with new {@code Record} and without duplicates.
     *
     * @param iterators original {@link Iterator} list
     * @return merged iterators
     */
    static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        Map<ByteBuffer, Record> out = new TreeMap<>(Comparator.naturalOrder());
        for (Iterator<Record> iterator : iterators) {
            Set<Record> records = new HashSet<>();
            while (iterator.hasNext()) {
                Record current = iterator.next();
                if (records.contains(current)) {
                    return iterator;
                }
                records.add(current);
                out.put(current.getKey(), current);
            }
        }
        return out.values().iterator();
    }

    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey);

    void upsert(Record record);

}
