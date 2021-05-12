package ru.mail.polis.lsm.incubos;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * In-memory implementation of {@link DAO}.
 *
 * @author incubos
 */
public class InMemoryDAO implements DAO {
    private final NavigableMap<ByteBuffer, Record> store = new ConcurrentSkipListMap<>();

    @Override
    public Iterator<Record> range(@Nullable final ByteBuffer fromKey, @Nullable final ByteBuffer toKey) {
        final Collection<Record> view;
        if (fromKey == null && toKey == null) {
            view = store.values();
        } else if (fromKey == null) {
            view = store.headMap(toKey).values();
        } else if (toKey == null) {
            view = store.tailMap(fromKey).values();
        } else {
            view = store.subMap(fromKey, toKey).values();
        }

        return view.stream().filter(r -> r.getValue() != null).iterator();
    }

    @Override
    public void upsert(final Record record) {
        store.put(record.getKey(), record);
    }

    @Override
    public void close() {
        store.clear();
    }
}
