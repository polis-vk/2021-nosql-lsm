package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

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

        int limit = Integer.MAX_VALUE / 128;

        Map<ByteBuffer, Record> map = new TreeMap<>();
        Record prevRecord = null;

        for (Iterator<Record> iterator : iterators) {
            int counter = 1;

            Stream.Builder<Record> builder = Stream.builder();

            while (iterator.hasNext()) {

                Record record = iterator.next();

                if (prevRecord != null && prevRecord.equals(record)) {
                    builder.add(record);

                    counter++;
                    if (counter > limit) {
                        Stream<Record> finalStream = builder.build();
                        return Stream.concat(map.values().stream(), finalStream)
                                .limit(limit).iterator();
                    }
                } else {
                    map.put(record.getKey(), record);
                }
                prevRecord = record;
            }
        }
        return map.values().iterator();
    }
}
