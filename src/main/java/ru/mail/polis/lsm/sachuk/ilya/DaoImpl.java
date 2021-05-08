package ru.mail.polis.lsm.sachuk.ilya;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DaoImpl implements DAO {

    private final SortedMap<ByteBuffer, Record> storage = new TreeMap<>();
    private final Lock lock = new ReentrantLock();

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        lock.lock();
        Map<ByteBuffer, Record> mapCopy = new TreeMap<>(map(fromKey, toKey));
        lock.unlock();
        return mapCopy.values().iterator();
    }

    @Override
    public void upsert(Record record) {
        lock.lock();
        if (record.getValue() != null) {
            storage.put(record.getKey(), record);
        } else {
            storage.remove(record.getKey());
        }
        lock.unlock();
    }

    @Override
    public void close() throws IOException {
        storage.clear();
    }

    private Map<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return storage;
        } else if (fromKey == null) {
            return storage.headMap(toKey);
        } else if (toKey == null) {
            return storage.tailMap(fromKey);
        }
        return storage.subMap(fromKey, toKey);
    }
}

