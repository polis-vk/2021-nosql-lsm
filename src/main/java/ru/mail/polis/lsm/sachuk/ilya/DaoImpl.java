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

public class DaoImpl implements DAO {

    private final SortedMap<ByteBuffer, Record> sortedMap = new TreeMap<>();

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return map(fromKey, toKey).values().iterator();
    }

    @Override
    public void upsert(Record record) {
        sortedMap.put(record.getKey(), record);
    }

    @Override
    public void close() throws IOException {
        sortedMap.clear();
    }

    private Map<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return sortedMap;
        } else if (fromKey == null) {
            return sortedMap.headMap(toKey);
        } else if (toKey == null) {
            return sortedMap.tailMap(fromKey);
        }
        return sortedMap.subMap(fromKey, toKey);
    }
}

