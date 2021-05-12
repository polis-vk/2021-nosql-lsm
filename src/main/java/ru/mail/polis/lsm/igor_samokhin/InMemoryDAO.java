package ru.mail.polis.lsm.igor_samokhin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import javax.annotation.Nullable;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

public class InMemoryDAO implements DAO {
    private final DAOConfig config;
    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();

    public InMemoryDAO(DAOConfig config) {
        this.config = config;
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return getSubMap(fromKey, toKey).values().iterator();
    }

    private SortedMap<ByteBuffer, Record> getSubMap(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return storage;
        } else if (fromKey == null) {
            return storage.headMap(toKey);
        } else if (toKey == null) {
            return storage.tailMap(fromKey);
        }
        return storage.subMap(fromKey,toKey);
    }

    @Override
    public void upsert(Record record) {
        if (record.getValue() == null) {
            Record r = storage.get(record.getKey());
            if (r != null) {
                storage.remove(r.getKey());
            }
        } else {
            storage.put(record.getKey(), record);
        }
    }

    @Override
    public void close() throws IOException {

    }
}
