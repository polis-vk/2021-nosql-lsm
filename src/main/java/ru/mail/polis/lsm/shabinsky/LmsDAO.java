package ru.mail.polis.lsm.shabinsky;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

public class LmsDAO implements DAO {

    private final SortedMap<ByteBuffer, Record> memoryStorage = new ConcurrentSkipListMap<>();
    private final ConcurrentLinkedDeque<SSTable> tables = new ConcurrentLinkedDeque<>();

    private final DAOConfig config;

    private static final long MEM_LIM = 20 * 1024 * 1024;
    private long memorySize;

    @GuardedBy("this")
    private Integer genIndex;

    /**
     * NotJustInMemoryDAO constructor.
     *
     * @param config {@link DAOConfig}
     * @throws IOException raises an exception
     */
    public LmsDAO(DAOConfig config) throws IOException {
        this.config = config;
        List<SSTable> list = SSTable.loadFromDir(config.getDir());
        this.genIndex = list.size();
        tables.addAll(list);
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        synchronized (this) {
            List<Iterator<Record>> iteratorList = sstableRange(fromKey, toKey);
            if (!memoryStorage.isEmpty()) {
                Iterator<Record> memoryRange = map(fromKey, toKey).values().stream()
                    .filter(record -> !record.isTombstone())
                    .collect(Collectors.toList())
                    .iterator();
                iteratorList.add(memoryRange);
            }

            return new MergeRecordIterator(iteratorList);
        }
    }

    @Override
    public void upsert(Record record) {
        synchronized (this) {

            updateMemorySize(record);
            if (memorySize >= MEM_LIM) {
                try {
                    flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (memoryStorage.get(record.getKey()) != null) {
                memoryStorage.remove(record.getKey());
            }
            memoryStorage.put(record.getKey(), record);
        }
    }

    private void updateMemorySize(Record record) {
        Record returnRecord = memoryStorage.get(record.getKey());
        boolean exist = returnRecord != null;

        if (record.isTombstone()) {
            if (exist) {
                memorySize -= (record.getKey().remaining() + 2 * Integer.BYTES);
            } else {
                memorySize += record.getKey().remaining() + 2 * Integer.BYTES;
            }
        } else {
            if (exist) {
                memorySize -=
                    (returnRecord.getKey().remaining() + returnRecord.getValue().remaining() + 2 * Integer.BYTES);
            }
            memorySize += record.getKey().remaining() + record.getValue().remaining() + 2 * Integer.BYTES;
        }
    }

    /**
     * GuardedBy("this").
     *
     * @throws IOException exception
     */
    @GuardedBy("this")
    public void flush() throws IOException {
        Path dir = config.getDir();
        String fileName = SSTable.NAME + genIndex++;

        SSTable ssTable = SSTable.write(memoryStorage.values().iterator(), dir, fileName);
        tables.add(ssTable);

        memorySize = 0;
        memoryStorage.clear();
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (!memoryStorage.isEmpty()) flush();
            for (SSTable ssTable : tables) ssTable.close();
        }
    }

    private List<Iterator<Record>> sstableRange(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(tables.size());
        for (SSTable ssTable : tables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return iterators;
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
