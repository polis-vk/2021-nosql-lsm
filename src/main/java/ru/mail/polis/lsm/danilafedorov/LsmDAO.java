package ru.mail.polis.lsm.danilafedorov;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

public class LsmDAO implements DAO {

    private final SortedMap<ByteBuffer, Record> memoryStorage = new ConcurrentSkipListMap<>();
    private ConcurrentLinkedDeque<SSTable> ssTables  = new ConcurrentLinkedDeque<>();

    private final DAOConfig config;

    private Integer memoryConsumption = 0;
    private static final Integer MEMORY_LIMIT = 1024 * 1024 * 32;
    static final String FILE_NAME = "SSTable_";
    private static final String COMPACTED_FILE_ENDING = "compacted";
    private static final String COMPACTED_FILE_NAME = FILE_NAME + COMPACTED_FILE_ENDING;

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

        final Iterator<Record> mergedRanges = mergeTwo(sstableRanges, memoryStorageRange);
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
    public void compact() {
        synchronized (this) {
            final Path pathTemp = getCompactedSSTablePath();
            try {
                flush();
                final Iterator<Record> it = new TombstoneSkippingIterator(sstableRanges(null, null));
                final SSTable ssTable = SSTable.write(pathTemp, it);
                ssTables.add(ssTable);
                close();

                restore();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        flush();

        for (SSTable ssTable : ssTables) {
            ssTable.close();
        }

        ssTables.clear();
    }

    private static boolean isCompacted(Path path) {
        String name = path.getFileName().toString();
        return name.contains(COMPACTED_FILE_ENDING);
    }

    private Path getCompactedSSTablePath() {
        return config.getDir().resolve(COMPACTED_FILE_NAME);
    }

    private void replaceCompactedSSTable() throws IOException {
        try (Stream<Path> paths = Files.list(config.getDir())) {
            Iterator<Path> it = paths
                    .filter(e -> !isCompacted(e))
                    .iterator();

            while (it.hasNext()) {
                Path path = it.next();
                Files.delete(path);
            }
        }

        final Path pathTemp = getCompactedSSTablePath();
        final Path indexPathTemp = SSTable.resolveIndexFilePath(pathTemp);

        final Path path = getNewSSTablePath();
        final Path indexPath = SSTable.resolveIndexFilePath(path);

        Files.copy(indexPathTemp, indexPath, StandardCopyOption.REPLACE_EXISTING);
        Files.move(pathTemp, path, StandardCopyOption.ATOMIC_MOVE);
        Files.delete(indexPathTemp);
    }

    private void restore() throws IOException {
        if (Files.exists(getCompactedSSTablePath())) {
            replaceCompactedSSTable();
        }

        ssTables = SSTable.loadFromDir(config.getDir());
    }

    private Iterator<Record> sstableRanges(ByteBuffer fromKey, ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(ssTables.size());
        for (SSTable ssTable : ssTables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return merge(iterators);
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

        final Path path = getNewSSTablePath();
        final Iterator<Record> iterator = memoryStorage.values().iterator();

        final SSTable ssTable = SSTable.write(path, iterator);
        memoryStorage.clear();
        memoryConsumption = 0;
        ssTables.add(ssTable);
    }

    private Path getNewSSTablePath() {
        final String name = FILE_NAME + ssTables.size();
        return config.getDir().resolve(name);
    }

    /**
     * Merges iterators.
     *
     * @param iterators List
     * @return Iterator
     */
    private static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        if (iterators.isEmpty()) {
            return Collections.emptyIterator();
        }
        if (iterators.size() == 1) {
            return iterators.get(0);
        }
        if (iterators.size() == 2) {
            return mergeTwo(iterators.get(0), iterators.get(1));
        }

        Iterator<Record> left = merge(iterators.subList(0, iterators.size() / 2));
        Iterator<Record> right = merge(iterators.subList(iterators.size() / 2, iterators.size()));
        return mergeTwo(left, right);
    }

    private static Iterator<Record> mergeTwo(Iterator<Record> left, Iterator<Record> right) {
        return new MergedRecordsIterator(left, right);
    }

    static class MergedRecordsIterator implements Iterator<Record> {

        private final Iterator<Record> it1;
        private final Iterator<Record> it2;
        private Record next1;
        private Record next2;

        public MergedRecordsIterator(final Iterator<Record> left, final Iterator<Record> right) {
            it1 = right;
            it2 = left;
            getNext1();
            getNext2();
        }

        @Override
        public boolean hasNext() {
            return next1 != null || next2 != null;
        }

        @Override
        public Record next() {
            Record returnRecord = null;

            if (hasNext()) {
                if (next2 == null) {
                    returnRecord = next1;
                    getNext1();
                } else if (next1 == null) {
                    returnRecord = next2;
                    getNext2();
                } else {
                    int compareResult = next1.getKey().compareTo(next2.getKey());

                    if (compareResult <= 0) {
                        returnRecord = next1;
                        getNext1();

                        if (compareResult == 0) {
                            getNext2();
                        }
                    } else {
                        returnRecord = next2;
                        getNext2();
                    }
                }
            }

            return returnRecord;
        }

        private void getNext1() {
            next1 = it1.hasNext() ? it1.next() : null;
        }

        private void getNext2() {
            next2 = it2.hasNext() ? it2.next() : null;
        }
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
