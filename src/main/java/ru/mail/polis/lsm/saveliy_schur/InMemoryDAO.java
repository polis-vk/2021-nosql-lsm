package ru.mail.polis.lsm.saveliy_schur;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDAO implements DAO {

    private final ConcurrentSkipListMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    private final DAOConfig config;

    public InMemoryDAO(DAOConfig config) {
        this.config = config;
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if(fromKey == null && toKey == null)
            return storage.values().stream().filter(x -> !x.isHidden()).iterator();
        else if(fromKey == null)
            return storage.headMap(toKey).values().stream().filter(x -> !x.isHidden()).iterator();
        else if(toKey == null)
            return storage.tailMap(fromKey).values().stream().filter(x -> !x.isHidden()).iterator();
        else
            return storage.subMap(fromKey, toKey).values().stream().filter(x -> !x.isHidden()).iterator();
    }

    @Override
    public void upsert(Record record) {
        if(record.getValue() == null){
            if(storage.containsKey(record.getKey()))
                storage.get(record.getKey()).hide();
            return;
        }
        storage.put(record.getKey(), record);
    }

    @Override
    public void close() throws IOException {

    }

    public DAOConfig getConfig() {
        return config;
    }
}
