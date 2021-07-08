package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;

public class DaoImpl implements DAO {

    private static final int MEMORY_LIMIT = 1024 * 1024 * 32;
    private final NavigableMap<ByteBuffer, Record> map = new ConcurrentSkipListMap<>();
    private final ConcurrentLinkedDeque<SSTable> tables = new ConcurrentLinkedDeque<>();
    private int memoryConsumption;

    private final DAOConfig config;

    @GuardedBy("this")
    private int nextSStableIndex;

    /**
     * Implementation of DAO with Persistence.
     */
    public DaoImpl(DAOConfig config) throws IOException {
        this.config = config;
        List<SSTable> ssTables = SSTable.loadFromDir(config.getDir());
        nextSStableIndex = ssTables == null ? 0 : ssTables.size();
        tables.addAll(ssTables);
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        synchronized (this) {
            List<Iterator<Record>> iterators = new ArrayList<>(tables.size() + 1);
            for (SSTable ssTable : tables) {
                iterators.add(ssTable.range(fromKey, toKey));
            }

            Iterator<Record> memoryRange = map(fromKey, toKey).values().iterator();

            iterators.add(memoryRange);
            Iterator<Record> merged = DAO.merge(iterators);
            return new FilterIterator(merged, toKey, true);
        }
    }

    @Override
    public Iterator<Record> descendingRange(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        synchronized (this) {
            List<Iterator<Record>> iterators = new ArrayList<>(tables.size() + 1);
            for (SSTable ssTable : tables) {
                iterators.add(ssTable.descendingRange(fromKey, toKey));
            }

            Iterator<Record> memoryRange = descendingMap(fromKey, toKey).values().iterator();

            iterators.add(memoryRange);
            Iterator<Record> merged = DAO.merge(iterators, false);
            return new FilterIterator(merged, fromKey, false);
        }
    }

    private SortedMap<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey,@Nullable ByteBuffer toKey) {
        if ((fromKey == null) && (toKey == null)) {
            return map;
        }

        if (fromKey == null) {
            return map.headMap(toKey);
        }

        if (toKey == null) {
            return map.tailMap(fromKey);
        }

        return map.subMap(fromKey, toKey);
    }

    private SortedMap<ByteBuffer, Record> descendingMap(@Nullable ByteBuffer fromKey,@Nullable ByteBuffer toKey) {
        NavigableMap<ByteBuffer, Record> descMap = map.descendingMap();

        if ((fromKey == null) && (toKey == null)) {
            return descMap;
        }

        if (fromKey == null) {
            return descMap.tailMap(toKey, true);
        }

        if (toKey == null) {
            return descMap.headMap(fromKey, true);
        }

        return descMap.subMap(toKey, true, fromKey, true);
    }

    @Override
    public void upsert(Record record) {
        synchronized (this) {
            map.put(record.getKey(), record);
            memoryConsumption += record.getSize();
            if (memoryConsumption > MEMORY_LIMIT) {
                try {
                    flush();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    @Override
    public void compact() throws IOException {
        synchronized (this) {
            Iterator<Record> result = range(null, null);
            boolean dataExists = result.hasNext() && !tables.isEmpty();
            SSTable compactSSTable = null;
            String compactFileName = "file_" + tables.size();
            String compactIdxName = "idx_" + tables.size();
            if (dataExists) {
                compactSSTable = makeSSTable(compactFileName, compactIdxName, result);
                tables.add(compactSSTable);
            }
            removeOldSSTables();
            if (dataExists) {
                map.clear();
                Files.move(config.getDir().resolve(compactFileName), config.getDir().resolve("file_0"));
                Files.move(config.getDir().resolve(compactIdxName), config.getDir().resolve("idx_0"));
                Objects.requireNonNull(compactSSTable).setFilePath(config.getDir().resolve("file_0"));
            }
        }
    }

    private void removeOldSSTables() throws IOException {
        if (tables.isEmpty()) {
            return;
        }

        String compactionFileNumber = tables.getLast().getFilePath().getFileName().toString().substring(5);
        for (SSTable table : tables) {
            if (!table.getFilePath().getFileName().toString().contains(compactionFileNumber)) {
                table.close();
                tables.remove(table);
                Files.deleteIfExists(table.getFilePath());
                Files.deleteIfExists(table.getIdxPath());
            }
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            flush();
            for (SSTable table : tables) {
                table.close();
            }
        }
    }

    @GuardedBy("this")
    private void flush() throws IOException {
        Iterator<Record> data = map.values().iterator();
        if (memoryConsumption > 0 && data.hasNext()) {
            tables.add(makeSSTable(
                    "file_" + nextSStableIndex,
                    "idx_" + nextSStableIndex,
                    data
            ));

            nextSStableIndex++;
            memoryConsumption = 0;
            map.clear();
        }
    }

    private SSTable makeSSTable(String fileName, String idxName, Iterator<Record> data) throws IOException {
        Path dir = config.getDir();
        Path file = dir.resolve(fileName);
        Path idx = dir.resolve(idxName);
        return SSTable.write(data, file, idx);
    }

}
