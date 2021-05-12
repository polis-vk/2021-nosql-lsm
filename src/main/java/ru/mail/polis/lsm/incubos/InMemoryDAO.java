package ru.mail.polis.lsm.incubos;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In-memory implementation of {@link DAO}.
 *
 * @author incubos
 */
public class InMemoryDAO implements DAO {
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    @GuardedBy("lock")
    private NavigableMap<ByteBuffer, Record> store = new ConcurrentSkipListMap<>();

    @Override
    public Iterator<Record> range(@Nullable final ByteBuffer fromKey, @Nullable final ByteBuffer toKey) {
        lock.readLock().lock();
        try {
            final NavigableMap<ByteBuffer, Record> copy = this.store;
            if (copy == null) {
                throw new IllegalStateException("Can't iterate closed DAO");
            }

            final SortedMap<ByteBuffer, Record> view;
            if (fromKey == null && toKey == null) {
                view = copy;
            } else if (fromKey == null) {
                view = copy.headMap(toKey);
            } else if (toKey == null) {
                view = copy.tailMap(fromKey);
            } else {
                view = copy.subMap(fromKey, toKey);
            }

            return view.values().iterator();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(final Record record) {
        lock.readLock().lock();
        try {
            final NavigableMap<ByteBuffer, Record> copy = this.store;
            if (copy == null) {
                throw new IllegalStateException("Can't modify closed DAO");
            }

            if (record.getValue() == null) {
                copy.remove(record.getKey());
            } else {
                copy.put(record.getKey(), record);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            store = null;
        } finally {
            lock.writeLock().unlock();
        }
    }
}
