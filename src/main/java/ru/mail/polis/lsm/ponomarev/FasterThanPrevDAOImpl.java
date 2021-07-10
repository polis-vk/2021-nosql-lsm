package ru.mail.polis.lsm.ponomarev;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class FasterThanPrevDAOImpl implements DAO {
    private static final int MEMORY_LIMIT = 12345;

    private final NavigableMap<ByteBuffer, Record> store;
    private final SSTable table;
    private volatile int storeSize;

    /**
     * @param config конфигурация дао
     * @throws IOException выбрасывает в случае чего вдруг, такое возможно
     */
    public FasterThanPrevDAOImpl(DAOConfig config) throws IOException {
        this.table = new SSTable(config.getDir());
        this.store = new ConcurrentSkipListMap<>();
        this.storeSize = 0;
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        try {
            var merged = DAO.merge(List.of(table.read(fromKey, toKey), store.values()
                    .stream()
                    .filter(r -> filterRecords(r, fromKey, toKey))
                    .iterator())
            );

            return merged;
        } catch (IOException e) {
            e.printStackTrace();
            return Collections.emptyIterator();
        }
    }

    private boolean filterRecords(Record record, ByteBuffer fromKey, ByteBuffer toKey) {
        if (record.isTombstone()) {
            return false;
        }

        if (fromKey == null && toKey == null) {
            return true;
        }

        if (fromKey == null) {
            return record.getKey().compareTo(toKey) <= 0;
        }

        if (toKey == null) {
            return record.getKey().compareTo(fromKey) >= 0;
        }

        return record.getKey().compareTo(fromKey) >= 0
                && record.getKey().compareTo(toKey) <= 0;
    }

    @Override
    public void upsert(Record record) {
        try {
            ByteBuffer key = record.getKey();
            ByteBuffer value = record.getValue();

            Record newRecord = value == null ? Record.tombstone(key) : Record.of(key, value);
            store.put(key, newRecord);

            updateStoreSize(newRecord);

            if (storeSize >= MEMORY_LIMIT) {
                synchronized (this) {
                    if (storeSize >= MEMORY_LIMIT) {
                        flushRecords();
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Store flushing are failed.", e.getCause());
        }
    }

    @Override
    public void close() throws IOException {
        table.flush(store.values());
    }

    private void flushRecords() throws IOException {
        table.flush(store.values());
        this.storeSize = 0;

    }

    private synchronized void updateStoreSize(Record record) {
        this.storeSize += sizeOf(record);
    }

    private int sizeOf(Record record) {
        ByteBuffer key = record.getKey();

        return key.remaining() + (record.isTombstone() ? 0 : record.getValue().remaining());
    }
}
