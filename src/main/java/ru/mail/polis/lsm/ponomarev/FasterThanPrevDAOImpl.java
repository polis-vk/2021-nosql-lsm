package ru.mail.polis.lsm.ponomarev;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public final class FasterThanPrevDAOImpl implements DAO {
    private static final int MEMORY_LIMIT = 9999999;

    private NavigableMap<ByteBuffer, Record> store;
    private final SSTable table;
    private volatile int storeSize;

    /**
     * Простая имплементация дао.
     *
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
            var recordsFromDisk = table.read(fromKey, toKey);
            var recordsFromStore = getStoredValues(fromKey, toKey);

            return DAO.merge(List.of(recordsFromDisk, recordsFromStore));
        } catch (IOException e) {
            throw new DaoException("Disk reading are failed.", e);
        }
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
            throw new DaoException("Store flushing are failed.", e);
        }
    }

    @Override
    public void close() throws IOException {
        table.flush(store.values().iterator());
        store = null;
    }

    private Iterator<Record> getStoredValues(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return store.values()
                .stream()
                .filter(r -> filterRecords(r, fromKey, toKey))
                .iterator();
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

    private void flushRecords() throws IOException {
        table.flush(store.values().iterator());
        store = new ConcurrentSkipListMap<>();
        storeSize = 0;
    }

    private synchronized void updateStoreSize(Record record) {
        storeSize += sizeOf(record);
    }

    private int sizeOf(Record record) {
        return record.getKey().remaining() + (record.isTombstone() ? 0 : record.getValue().remaining());
    }
}
