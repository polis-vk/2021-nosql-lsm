package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

public class DaoImpl implements DAO{
    DAOConfig config;
    NavigableMap<ByteBuffer, Record> map = new ConcurrentSkipListMap<>();

    public DaoImpl(DAOConfig config) {
        this.config = config;
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return  map(fromKey, toKey).values().stream()
                .filter(record -> record.getValue() != null)
                .iterator();
    }

    private SortedMap<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey,@Nullable ByteBuffer toKey) {
        if ((fromKey == null) && (toKey == null))
                return map;

        if (fromKey == null)
            return map.headMap(toKey);

        if (toKey == null)
            return map.tailMap(fromKey);

        return map.subMap(fromKey, toKey);
    }

    @Override
    public void upsert(Record record) {
        map.put(record.getKey(), record);
    }

    @Override
    public void close() throws IOException {

    }
}
