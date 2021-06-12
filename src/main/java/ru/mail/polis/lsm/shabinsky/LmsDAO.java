package ru.mail.polis.lsm.shabinsky;

import ru.mail.polis.lsm.*;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;

public class LmsDAO implements DAO {

    private final SortedMap<ByteBuffer, Record> memoryStorage = new ConcurrentSkipListMap<>();
    private final ConcurrentLinkedDeque<SSTable> tables = new ConcurrentLinkedDeque<>();

    private final DAOConfig config;

    private final long MEMORY_LIMIT = 127 * 1024 * 1024;
    private long memorySize = 0;
    private Integer generation;

    /**
     * NotJustInMemoryDAO constructor.
     *
     * @param config {@link DAOConfig}
     * @throws IOException raises an exception
     */
    public LmsDAO(DAOConfig config) throws IOException {
        this.config = config;
        tables.addAll(SSTable.loadFromDir(config.getDir()));
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        synchronized (this) {
            Iterator<Record> sstableRange;
            sstableRange = sstableRange(fromKey, toKey);
            Iterator<Record> memoryRange = map(fromKey, toKey).values().iterator();
            return new MergeRecordIterator(List.of(sstableRange, memoryRange));
        }
    }

    @Override
    public void upsert(Record record) {
        synchronized (this) {
            memorySize += record.getKey().remaining() + record.getValue().remaining();
            if (memorySize > MEMORY_LIMIT) {
                try {
                    flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        if (record.getValue() == null) {
            memoryStorage.remove(record.getKey());
        } else {
            memoryStorage.put(record.getKey(), record);
        }
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    public void flush() throws IOException {
        SSTable ssTable = SSTable.write(
            memoryStorage.values().iterator(),
            config.getDir().resolve(generation.toString() + ".tmp"));

        tables.add(ssTable);
        /*
            TODO
            something
         */

        memoryStorage.clear();
    }

    private Iterator<Record> sstableRange(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(tables.size());
        for (SSTable ssTable : tables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return new MergeRecordIterator(iterators);
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
