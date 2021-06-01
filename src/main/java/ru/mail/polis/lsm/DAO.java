package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toMap;

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
     * Merge iterators.
     */
    static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        var map = new TreeMap<ByteBuffer, Record>();
        Record lastRec = null;
        for (var item : iterators) {
            do {
                var temp = item.next();
                if(temp.equals(lastRec)) {
                    return iterators.stream()
                            .flatMap(e ->
                                    StreamSupport.stream(
                                            Spliterators.spliteratorUnknownSize(e, Spliterator.ORDERED),
                                            false))
                            .collect(toMap(Record::getKey, record -> record, (recordL, recordR) -> recordR,
                                    ConcurrentSkipListMap::new))
                            .values()
                            .iterator();
                }
                map.put(temp.getKey(), temp);
                lastRec = temp;
            } while (item.hasNext());
        }
        return map.values().iterator();
    }
}
