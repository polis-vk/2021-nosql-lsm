package ru.mail.polis.lsm.danilafedorov;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;

public class LsmDAO implements DAO {

    private final SortedMap<ByteBuffer, Record> memoryStorage = new ConcurrentSkipListMap<>();
    private ConcurrentLinkedDeque<SSTable> ssTables;

    private final DAOConfig config;

    private Integer memoryConsumption = 0;
    private static final Integer MEMORY_LIMIT = 1024 * 1024 * 32;

    static final String FILE_NAME = "SSTable_";

    /**
     * Class constructor identifying directory of DB location.
     *
     * @param config contains directory path of DB location
     * @throws IOException If an input exception occurred
     */
    public LsmDAO(final DAOConfig config) throws IOException {
        this.config = config;
        restore();
    }

    @Override
    public Iterator<Record> range(@Nullable final ByteBuffer fromKey, @Nullable final ByteBuffer toKey) {
        final Iterator<Record> sstableRanges;
        synchronized (this) {
            sstableRanges = sstableRanges(fromKey, toKey);
        }
        final Iterator<Record> memoryStorageRange = memoryStorageRange(fromKey, toKey);

        final Iterator<Record> mergedRanges = DAO.mergeTwo(sstableRanges, memoryStorageRange);
        return new TombstoneSkippingIterator(mergedRanges);
    }

    @Override
    public void upsert(final Record record) {
        synchronized (this) {
            int size = sizeOf(record);
            memoryConsumption += size;

            if (memoryConsumption > MEMORY_LIMIT) {
                try {
                    flush();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            memoryConsumption += size;
        }
        memoryStorage.put(record.getKey(), record);
    }

    @Override
    public void close() throws IOException {
        flush();

        for (SSTable ssTable : ssTables) {
            ssTable.close();
        }
    }

    private void restore() throws IOException {
        ssTables = SSTable.loadFromDir(config.getDir());
    }

    private Iterator<Record> sstableRanges(ByteBuffer fromKey, ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(ssTables.size());
        for (SSTable ssTable : ssTables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return DAO.merge(iterators);
    }

    private Iterator<Record> memoryStorageRange(ByteBuffer fromKey, ByteBuffer toKey) {
        return map(fromKey, toKey).values().iterator();
    }

    private SortedMap<ByteBuffer, Record> map(@Nullable final ByteBuffer fromKey, @Nullable final ByteBuffer toKey) {
        if (fromKey == null) {
            if (toKey == null) {
                return memoryStorage;
            }

            return memoryStorage.headMap(toKey);
        }

        if (toKey == null) {
            return memoryStorage.tailMap(fromKey);
        }

        return memoryStorage.subMap(fromKey, toKey);
    }

    private Integer sizeOf(Record record) {
        int keySize = record.getKey().remaining();
        int valueSize = record.isTombstone() ? 0 : record.getValue().remaining();
        return keySize + valueSize;
    }

    private void flush() throws IOException {
        if (memoryStorage.isEmpty()) {
            return;
        }

        final String name = FILE_NAME + ssTables.size();
        final Path path = config.getDir().resolve(name);
        final Iterator<Record> iterator = memoryStorage.values().iterator();

        final SSTable ssTable = SSTable.write(path, iterator);
        memoryStorage.clear();
        memoryConsumption = 0;
        ssTables.add(ssTable);
    }

    static class TombstoneSkippingIterator implements Iterator<Record> {

        final Iterator<Record> it;
        Record current;

        TombstoneSkippingIterator(Iterator<Record> iterator) {
            it = iterator;
        }

        @Override
        public boolean hasNext() {
            skipTombstones();
            return current != null;
        }

        @Override
        public Record next() {
            skipTombstones();
            Record temp = current;
            current = null;
            return temp;
        }

        private void skipTombstones() {
            if (current == null) {
                getRecord();
            }
            while (current != null && current.isTombstone()) {
                getRecord();
            }
        }

        private void getRecord() {
            current = it.hasNext() ? it.next() : null;
        }
    }
}
