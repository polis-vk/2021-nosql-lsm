package ru.mail.polis.lsm.segu.dao;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.segu.model.SSTable;
import ru.mail.polis.lsm.segu.model.SSTablePath;
import ru.mail.polis.lsm.segu.service.SSTableService;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Implementation of NotInMemory DAO.
 */

public class LSMDao implements DAO {

    private final SSTableService ssTableService = new SSTableService();

    private final ConcurrentLinkedDeque<SSTable> ssTables = new ConcurrentLinkedDeque<>();
    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    private int storageSize;

    private final DAOConfig config;

    private static final int DEFAULT_THRESHOLD = 1024 * 1024 * 500; // 500 MB
    private final int threshold;
    private int ssTableNextIndex = 0;


    /**
     * Constructor.
     *
     * @param config - конфиг
     */

    public LSMDao(DAOConfig config) {
        this(config, DEFAULT_THRESHOLD);
    }

    public LSMDao(DAOConfig config, int threshold) {
        this.config = config;
        this.threshold = threshold;
    }

    @Override
    public Iterator<Record> range(@Nullable final ByteBuffer fromKey, @Nullable final ByteBuffer toKey) {
        return map(fromKey, toKey).values().stream()
                .filter(record -> record.getValue() != null)
                .iterator();
    }

    @Override
    public void upsert(final Record record) {
        synchronized (this) {
            if (storageSize >= threshold) {
                try {
                    flush();
                } catch (IOException e) {
                    e.printStackTrace();
                    System.err.println("Failed to flush");
                }
                clear();
            } else {
                storage.put(record.getKey(), record);
                storageSize += record.size();
            }
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            flush();
            clear();
        }
    }

    private void flush() throws IOException {
        SSTablePath ssTablePath = ssTableService.resolvePath(config,
                ssTableNextIndex, SSTable.FILE_PREFIX, SSTable.INDEX_FILE_PREFIX);
        ssTableNextIndex++;
        SSTable ssTable = new SSTable(ssTablePath.getFilePath(), ssTablePath.getIndexFilePath(), storage);
        ssTables.add(ssTable);
        ssTableService.writeTableAndIndexFile(ssTable);
    }

    private void clear() {
        storage.clear();
        storageSize = 0;
    }

    private SortedMap<ByteBuffer, Record> map(@Nullable final ByteBuffer fromKey, @Nullable final ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return storage;
        }
        if (fromKey == null) {
            return storage.headMap(toKey);
        }
        if (toKey == null) {
            return storage.tailMap(fromKey);
        }
        return storage.subMap(fromKey, toKey);
    }


}
