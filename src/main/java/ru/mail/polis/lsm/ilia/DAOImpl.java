package ru.mail.polis.lsm.ilia;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nonnull;
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

public class DAOImpl implements DAO {

    private static final int MEMORY_LIM = 1024 * 1024 * 20;
    private final SortedMap<ByteBuffer, Record> memoryStorage = new ConcurrentSkipListMap<>();
    private final DAOConfig config;
    private final ConcurrentLinkedDeque<SSTable> tables = new ConcurrentLinkedDeque<>();
    private int nextSStableIndex;
    private int memorySize;

    /**
     * Create DAOImpl constructor.
     *
     * @param config contains directory with file
     */
    public DAOImpl(DAOConfig config) throws IOException {
        this.config = config;
        List<SSTable> ssTables = SSTable.loadFromDir(config.getDir());
        nextSStableIndex = ssTables.size();
        tables.addAll(ssTables);
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        synchronized (this) {
            Iterator<Record> ssTableRanges = sstableRanges(fromKey, toKey);
            Iterator<Record> memoryRange = map(fromKey, toKey).values().iterator();
            Iterator<Record> mergeTwo = new MergeTwo(new PeekingIterator(ssTableRanges), new PeekingIterator(memoryRange));
            return new FilterIterator(mergeTwo);
        }
    }

    @Override
    public void upsert(@Nonnull Record record) {
        synchronized (this) {
            memorySize += sizeOf(record);
            if (memorySize >= MEMORY_LIM) {
                try {
                    flush();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }

        memoryStorage.put(record.getKey(), record);
    }

    private Integer sizeOf(Record record) {
        int keySize = record.getKey().remaining();
        int valueSize = 0;
        if (!record.isTombstone()) {
            valueSize = record.getValue().remaining();
        }
        return keySize + valueSize;
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (!memoryStorage.isEmpty()) {
                flush();
            }
            for (SSTable ssTable : tables) {
                ssTable.close();
            }
        }
    }

    private void flush() throws IOException {
        Path dir = config.getDir();
        Path file = dir.resolve("file_" + nextSStableIndex);
        nextSStableIndex++;

        SSTable ssTable = SSTable.write(memoryStorage.values().iterator(), file);
        tables.add(ssTable);
        memoryStorage.clear();
        memorySize = 0;
    }

    private Iterator<Record> sstableRanges(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(tables.size());
        for (SSTable ssTable : tables) {
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
}
