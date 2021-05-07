package ru.mail.polis.lsm.segu;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryImpl implements DAO {

    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();

    @Override
    public Iterator<Record> range(@Nullable final ByteBuffer fromKey, @Nullable final ByteBuffer toKey) {
        return map(fromKey, toKey).values().stream()
                .filter(record -> record.getValue() != null)
                .iterator();
    }

    @Override
    public void upsert(final Record record) {
        storage.put(record.getKey(), record);
    }

    @Override
    public void close() {
        // Currently not implemented
    }

    private SortedMap<ByteBuffer, Record> map(@Nullable final ByteBuffer fromKey, @Nullable final ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
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
}
