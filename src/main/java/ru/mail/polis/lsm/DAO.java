package ru.mail.polis.lsm;

import ru.mail.polis.lsm.saveliyschur.sstservice.SSTable;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Minimal database API.
 */
public interface DAO extends Closeable {
    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey);

    void upsert(Record record);

    static ByteBuffer nextKey(ByteBuffer buffer) {
        ByteBuffer result = ByteBuffer.allocate(buffer.remaining() + 1);

        int position = buffer.position();

        result.put(buffer);
        result.put(Byte.MIN_VALUE);

        buffer.position(position);
        result.rewind();

        return result;
    }
}
