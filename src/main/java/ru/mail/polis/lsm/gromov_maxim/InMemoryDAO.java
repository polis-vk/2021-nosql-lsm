package ru.mail.polis.lsm.gromov_maxim;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;

public class InMemoryDAO implements DAO {

    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    ReentrantLock lock = new ReentrantLock();

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        lock.lock();
        try {
            return map(fromKey, toKey).values().iterator();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void upsert(Record record) {
        lock.lock();
        try {
            if (record.getValue() == null) {
                storage.remove(record.getKey());
            } else {
                storage.put(record.getKey(), record);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        storage.clear();
    }

    private SortedMap<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
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
