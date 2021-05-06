package ru.mail.polis.lsm.timatifey;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDAO implements DAO {

    private final NavigableMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return getSortedMap(fromKey, toKey).values().iterator();
    }

    private SortedMap<ByteBuffer, Record> getSortedMap(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if ((fromKey == null) && (toKey == null)) {
            return storage;
        }
        if (fromKey == null) {
            return storage.headMap(toKey);
        }
        if (toKey == null) {
            return storage.tailMap(fromKey);
        }
        return storage.subMap(fromKey, toKey);
    }

    @Override
    public void upsert(Record record) {
        if (record.getValue() == null) {
            storage.remove(record.getKey());
            return;
        }
        storage.put(record.getKey(), record);
    }

    @SuppressWarnings("unused")
    @Override
    public void close() throws IOException {
    }
}
