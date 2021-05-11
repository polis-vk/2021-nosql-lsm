package ru.mail.polis.lsm.veronika_peutina;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDAO implements DAO {

    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return getSubMap(fromKey, toKey).values().iterator();
    }

    private SortedMap<ByteBuffer, Record> getSubMap(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
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

    @Override
    public void upsert(Record record) {
        if (record.getKey() != null) {
            if (record.getValue() != null) {
                storage.put(record.getKey(), record);
            } else {
                storage.remove(record.getKey());
            }
        }
    }

    @Override
    public void close() throws IOException {
        storage.clear();
    }
}
