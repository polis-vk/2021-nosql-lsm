package ru.mail.polis.lsm.sachuk.ilya;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class DaoImpl implements DAO {

    private final SortedMap<ByteBuffer, Record> sortedMap = new TreeMap<>();

    @Override
    public synchronized Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        Map<ByteBuffer, Record> mapCopy = new TreeMap<>(map(fromKey, toKey));
        return mapCopy.values().iterator();
    }

    @Override
    public synchronized void upsert(Record record) {
        if (record.getValue() != null) {
            sortedMap.put(record.getKey(), record);
        } else {
            sortedMap.remove(record.getKey());
        }
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

