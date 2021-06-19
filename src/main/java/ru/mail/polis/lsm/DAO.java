package ru.mail.polis.lsm;

import ru.mail.polis.lsm.ilia.MergeTwo;
import ru.mail.polis.lsm.ilia.PeekingIterator;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

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
     * Merges iterators.
     *
     * @param iterators iterators List
     * @return result Iterator
     */
    static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        switch (iterators.size()) {
            case (0):
                return Collections.emptyIterator();
            case (1):
                return iterators.get(0);
            case (2):
                return new MergeTwo(new PeekingIterator(iterators.get(0)), new PeekingIterator(iterators.get(1)));
            default:
                Iterator<Record> left = merge(iterators.subList(0, iterators.size() / 2));
                Iterator<Record> right = merge(iterators.subList(iterators.size() / 2, iterators.size()));

                return new MergeTwo(new PeekingIterator(left), new PeekingIterator(right));
        }
    }

    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey);

    void upsert(Record record) throws UncheckedIOException;
}
