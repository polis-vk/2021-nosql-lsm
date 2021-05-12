package ru.mail.polis.lsm.incubos;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * In-memory implementation of {@link DAO}.
 *
 * @author incubos
 */
public class InMemoryDAO implements DAO {
    private static final NavigableMap<ByteBuffer, Record> CLOSED = Collections.emptyNavigableMap();

    private volatile NavigableMap<ByteBuffer, Record> store = new ConcurrentSkipListMap<>();

    @Override
    public Iterator<Record> range(@Nullable final ByteBuffer fromKey, @Nullable final ByteBuffer toKey) {
        final NavigableMap<ByteBuffer, Record> store = this.store;
        if (store == CLOSED) {
            throw new IllegalStateException("Already closed");
        }

        final SortedMap<ByteBuffer, Record> view;
        if (fromKey == null && toKey == null) {
            view = store;
        } else if (fromKey == null) {
            view = store.headMap(toKey);
        } else if (toKey == null) {
            view = store.tailMap(fromKey);
        } else {
            view = store.subMap(fromKey, toKey);
        }

        return view.values().iterator();
    }

    @Override
    public void upsert(final Record record) {
        if (record.getValue() == null) {
            store.remove(record.getKey());
        } else {
            store.put(record.getKey(), record);
        }
    }

    @Override
    public void close() {
        store = CLOSED;
    }
}
