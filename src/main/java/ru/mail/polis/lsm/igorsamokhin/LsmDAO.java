package ru.mail.polis.lsm.igorsamokhin;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

@SuppressWarnings("JdkObsolete")
public class LsmDAO implements DAO {
    private static final int MEMORY_LIMIT = 1024 * 1024 * 32;
    private static final String FILE_PREFIX = "SSTable_";
    private Integer currentTableN;

    private final SortedMap<ByteBuffer, Record> memoryStorage = new ConcurrentSkipListMap<>();
    private final List<SSTable> ssTables;
    private final DAOConfig config;
    private int memoryConsumption;

    private Path filePath;

    /**
     * Create DAO object.
     *
     * @param config - objects contains directory with data files
     */
    public LsmDAO(DAOConfig config) throws IOException {
        this.config = config;
        memoryConsumption = 0;

        ssTables = SSTable.loadFromDir(config.getDir());
        currentTableN = ssTables.size();

        filePath = getNewFileName();
    }

    private Path getNewFileName() {
        String binary = Long.toBinaryString(currentTableN);
        int leadingN = 64 - binary.length();

        String builder = "0".repeat(leadingN) + binary;
        String name = FILE_PREFIX.concat(builder);
        ++currentTableN;
        return config.getDir().resolve(name);
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey)
            throws UncheckedIOException {
        synchronized (this) {
            Iterator<Record> memoryRange = SSTable.getSubMap(memoryStorage, fromKey, toKey).values().iterator();
            Iterator<Record> sstableRanges = sstableRanges(fromKey, toKey);
            return LsmDAO.merge(List.of(sstableRanges, memoryRange));
        }
    }

    @Override
    public void upsert(Record record) throws UncheckedIOException {
        synchronized (this) {
            int size = sizeOf(record);
            if (memoryConsumption + size > MEMORY_LIMIT) {
                try {
                    flush();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            memoryConsumption += size;
            memoryStorage.put(record.getKey(), record);
        }
    }

    private int sizeOf(Record record) {
        int keyCapacity = record.getKey().capacity();
        ByteBuffer value = record.getValue();
        int valueCapacity = (value == null) ? 0 : value.capacity();
        return keyCapacity + valueCapacity;
    }

    @GuardedBy("this")
    private void flush() throws IOException {
        if (memoryConsumption == 0) {
            return;
        }
        SSTable.write(memoryStorage.values().iterator(), filePath);
        SSTable ssTable = SSTable.loadFromFile(filePath);
        ssTables.add(ssTable);
        memoryStorage.clear();
        memoryConsumption = 0;
        filePath = getNewFileName();
    }

    private Iterator<Record> sstableRanges(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(ssTables.size());
        for (SSTable sstable : ssTables) {
            iterators.add(sstable.range(fromKey, toKey));
        }
        return LsmDAO.merge(iterators);
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            flush();
            if (ssTables.isEmpty()) {
                return;
            }

            for (SSTable ssTable : ssTables) {
                ssTable.close();
            }
        }
    }

    @Override
    public void compact() throws IOException {
        synchronized (this) {
            Path compactFile = SSTable.compact(config.getDir(), this.range(null, null));

            if (compactFile == null) {
                return;
            }

            for (SSTable ssTable : ssTables) {
                ssTable.close();
            }
            ssTables.clear();
            SSTable.prepareDirectory(config.getDir());

            ssTables.addAll(SSTable.loadFromDir(config.getDir()));

            currentTableN = ssTables.size();
            filePath = getNewFileName();
        }
    }

    /**
     * Do merge iterators into one iterator.
     */
    public static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        PriorityQueue<Entry> queue = new PriorityQueue<>((a, b) -> {
            int i = a.prevRecord.getKey().compareTo(b.prevRecord.getKey());
            if (i == 0) {
                return a.order < b.order ? 1 : -1;
            }
            return i;
        });

        for (int i = 0; i < iterators.size(); i++) {
            Iterator<Record> it = iterators.get(i);
            if (it.hasNext()) {
                queue.add(new Entry(it, it.next(), i));
            }
        }

        return new Iterator<Record>() {
            @Override
            public boolean hasNext() {
                checkTombstones(queue);

                return !queue.isEmpty();
            }

            @Override
            public Record next() {
                checkTombstones(queue);

                Entry poll = queue.poll();
                if (poll == null) {
                    return null;
                }
                clearQueue(queue, poll);
                Record record = poll.prevRecord;
                if (poll.iterator.hasNext()) {
                    poll.prevRecord = poll.iterator.next();
                    queue.add(poll);
                }

                return record;
            }
        };
    }

    /**
     * Skip all first tombstones.
     */
    private static void checkTombstones(PriorityQueue<Entry> queue) {
        while (!queue.isEmpty() && (queue.peek().prevRecord.getValue() == null)) {
            Entry head = queue.poll();

            clearQueue(queue, head);
            if (head.iterator.hasNext()) {
                head.prevRecord = head.iterator.next();
                queue.add(head);
            }
        }
    }

    /**
     * Delete first N elements of the queue, which are equals with given.
     */
    private static void clearQueue(PriorityQueue<Entry> queue, Entry entry) {
        while (!queue.isEmpty() && (queue.peek().prevRecord.getKey().compareTo(entry.prevRecord.getKey()) == 0)) {
            Entry head = queue.poll();

            if (head != null && head.iterator.hasNext()) {
                head.prevRecord = head.iterator.next();
                queue.add(head);
            }
        }
    }
}
