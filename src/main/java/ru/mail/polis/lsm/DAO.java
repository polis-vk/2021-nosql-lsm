package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
        return iterators.stream()
                .flatMap(recordIterator ->
                        StreamSupport.stream(
                                Spliterators.spliteratorUnknownSize(recordIterator, Spliterator.ORDERED),
                                false))
                .collect(
                        Collectors.toMap(
                                Record::getKey,
                                Function.identity(),
                                (record1, record2) -> record2,
                                TreeMap::new))
                .values()
                .iterator();
    }

    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey);

    void upsert(Record record);

}
