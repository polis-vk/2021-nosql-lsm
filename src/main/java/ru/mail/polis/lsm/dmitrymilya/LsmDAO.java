package ru.mail.polis.lsm.dmitrymilya;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
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

/**
 * Persistent DAO implementation.
 */
public class LsmDAO implements DAO {
    private static final int memoryLimit = 1024 * 1024;
    private final SortedMap<ByteBuffer, Record> memoryStorage = new ConcurrentSkipListMap<>();
    private final ConcurrentLinkedDeque<SSTable> ssTables = new ConcurrentLinkedDeque<>();
    private final DAOConfig config;

    @GuardedBy("this")
    private int nextSSTableIndex;
    private int memoryConsumption;

    /**
     * Constructs a Persistent DAO object, getting data from the save file.
     *
     * @param config DAO config
     */
    public LsmDAO(DAOConfig config) throws IOException {
        this.config = config;
        List<SSTable> loadedSSTables = SSTable.loadFromDir(config.getDir());
        nextSSTableIndex = loadedSSTables.size();
        ssTables.addAll(loadedSSTables);
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        synchronized (this) {
            Iterator<Record> ssTableRanges = ssTableRanges(fromKey, toKey);
            Iterator<Record> memoryRange = new MemoryStorageIterator(map(fromKey, toKey).values().iterator());

            return DAO.mergeTwoIterators(ssTableRanges, memoryRange);
        }
    }

    private Iterator<Record> ssTableRanges(ByteBuffer fromKey, ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(ssTables.size());
        for (SSTable ssTable : ssTables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }

        return DAO.merge(iterators);
    }

    private SortedMap<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return memoryStorage;
        }
        if (fromKey == null) {
            return memoryStorage.headMap(toKey);
        }
        if (toKey == null) {
            return memoryStorage.tailMap(fromKey);
        }

        return memoryStorage.subMap(fromKey, toKey);
    }

    @Override
    public void upsert(@Nonnull Record record) {
        synchronized (this) {
            memoryConsumption += sizeOf(record);
            if (memoryConsumption > memoryLimit) {
                try {
                    flush();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }

        memoryStorage.put(record.getKey(), record);
    }

    private int sizeOf(Record record) {
        ByteBuffer key = record.getKey();
        ByteBuffer value = record.getValue();
        int keySize = 0;
        int valueSize = 0;
        if (key != null) {
            keySize = key.remaining();
            if (value != null) {
                valueSize = value.remaining();
            }
        }
        return keySize + valueSize;
    }

    @GuardedBy("this")
    private void flush() throws IOException {
        Path dir = config.getDir();
        Path file = dir.resolve("file_" + nextSSTableIndex++);
        SSTable ssTable = SSTable.write(memoryStorage.values().iterator(), file);
        ssTables.add(ssTable);
        memoryStorage.clear();
        memoryConsumption = 0;
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (memoryConsumption > 0) {
                flush();
            }
            ssTables.forEach(SSTable::cleanMappedByteBuffer);
        }
    }


    static class MemoryStorageIterator implements Iterator<Record> {

        private final Iterator<Record> iterator;

        private Record next;

        public MemoryStorageIterator(Iterator<Record> iterator) {
            this.iterator = iterator;
            next = iterator.hasNext() ? iterator.next() : null;
            while (next != null && next.isTombstone()) {
                next = iterator.hasNext() ? iterator.next() : null;
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public Record next() {
            Record toReturn = next;
            next = iterator.hasNext() ? iterator.next() : null;
            while (next != null && next.isTombstone()) {
                next = iterator.hasNext() ? iterator.next() : null;
            }
            return toReturn;
        }
    }
}
