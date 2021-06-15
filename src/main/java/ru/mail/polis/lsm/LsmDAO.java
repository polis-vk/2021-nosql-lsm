package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;

public class LsmDAO implements DAO {

    private static final Method CLEAN;
    private static final String SAVE_FILE_NAME = "file_";

    static {
        try {
            Class<?> fileChannelImplClass = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = fileChannelImplClass.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    private final ConcurrentLinkedDeque<SSTable> ssTables = new ConcurrentLinkedDeque<>();
    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    private final DAOConfig config;
    private final int MEMORY_SIZE = 1024 * 1024 * 32;
    private int memoryConsumption;

    /**
     * Implementation of DAO that save data to the memory.
     */
    public LsmDAO(DAOConfig config) throws IOException {
        this.config = config;
        List<SSTable> ssTables = SSTable.loadFromDir(config.getDir());
        this.ssTables.addAll(ssTables);
        memoryConsumption = 0;
    }

    /**
     * Merge k iterators to one.
     *
     * @param iterators original {@link Iterator} list
     * @return merged iterator
     */
    static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        if (iterators.isEmpty()) {
            return Collections.emptyIterator();
        }
        if (iterators.size() == 1) {
            return new FilterTombstoneIterator(iterators.get(0));
        }
        return new FilterTombstoneIterator(new MyIterator(iterators));
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        synchronized (this) {
            List<Iterator<Record>> iterators = new ArrayList<>();

            for (SSTable ssTable : ssTables) {
                    iterators.add(ssTable.range(fromKey, toKey));
            }

            if(!storage.isEmpty()) {
                iterators.add(map(fromKey, toKey).values().iterator());
            }

            return merge(iterators);
        }
    }

    @Override
    public void upsert(Record record) {
        synchronized (this) {

            memoryConsumption += sizeOf(record);
            storage.put(record.getKey(), record);

            try {
                if (memoryConsumption > MEMORY_SIZE) {
                    flush();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private int sizeOf(Record record) {
        if (record.getValue() == null) {
            return record.getKey().capacity();
        }
        return record.getKey().capacity() + record.getValue().capacity();
    }

    private void flush() throws IOException {
        if (storage.isEmpty()) {
            return;
        }
        final Path fileName =
                config.getDir().resolve(SAVE_FILE_NAME + ssTables.size() + 1);
        final SSTable ssTable = SSTable.write(storage.values().iterator(), fileName);
        ssTables.add(ssTable);
        storage.clear();
        memoryConsumption = 0;
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            flush();

            for (SSTable table : ssTables) {
                table.close();
            }
        }
    }

    private SortedMap<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
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

    static class Pair {

        Record record;
        Iterator<Record> iterator;

        Pair(Record record, Iterator<Record> iterator) {
            this.record = record;
            this.iterator = iterator;
        }
    }

    static class MyIterator implements Iterator<Record> {

        List<Pair> nextRecordList = new ArrayList<>();
        List<Iterator<Record>> iterators;

        public MyIterator(List<Iterator<Record>> iterators) {
            this.iterators = iterators;
            for (Iterator<Record> i : this.iterators) {
                if (i.hasNext()) {
                    nextRecordList.add(new Pair(i.next(), i));
                }
            }
        }

        @Override
        public boolean hasNext() {
            for (Pair pair : nextRecordList) {
                if (pair.iterator != null) {
                    return true;
                }
            }
            return false;
        }

        private void updateIteratorWithMinRecord(int minIndex) {
            Iterator<Record> iteratorToUpdate = nextRecordList.get(minIndex).iterator;
            if (iteratorToUpdate != null && minIndex != -1) {
                if (iteratorToUpdate.hasNext()) {
                    nextRecordList.set(minIndex, new Pair(iteratorToUpdate.next(), iteratorToUpdate));
                } else {
                    nextRecordList.set(minIndex, new Pair(null, null));
                }
            }
        }

        private void deleteEqualElementsInNextRecordList(int minIndex, Record minRecord) {
            for (int i = 0; i < nextRecordList.size(); ++i) {
                if (i == minIndex) {
                    continue;
                }

                Pair pair = nextRecordList.get(i);
                while (pair.iterator != null && minRecord != null && pair.record.getKey().equals(minRecord.getKey())) {
                    if (pair.iterator.hasNext()) {
                        pair.record = pair.iterator.next();
                    } else {
                        pair.iterator = null;
                        pair.record = null;
                    }
                }
            }
        }

        @Override
        public Record next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Record minRecord = null;
            int minIndex = -1;
            for (int i = 0; i < nextRecordList.size(); ++i) {
                Record currentRecord = nextRecordList.get(i).record;
                if (currentRecord != null
                        && (minRecord == null || minRecord.getKey().compareTo(currentRecord.getKey()) >= 0)) {
                    minRecord = currentRecord;
                    minIndex = i;
                }
            }

            updateIteratorWithMinRecord(minIndex);
            deleteEqualElementsInNextRecordList(minIndex, minRecord);

            return minRecord;
        }
    }

    static class FilterTombstoneIterator implements Iterator<Record> {

        private final Iterator<Record> mergedIterator;

        private Record peek;

        FilterTombstoneIterator(Iterator<Record> iterator) {
            this.mergedIterator = iterator;
            updatePeek();
        }

        private void updatePeek() {
            if (mergedIterator.hasNext()) {
                peek = mergedIterator.next();
            } else {
                peek = null;
            }
            while (mergedIterator.hasNext() && peek != null) {
                if (peek.isTombstone()) {
                    peek = mergedIterator.next();
                } else {
                    break;
                }
            }
            if (peek != null && peek.isTombstone()) {
                peek = null;
            }
        }

        @Override
        public boolean hasNext() {
            return peek != null;
        }

        @Override
        public Record next() {
            if(!hasNext()) {
                throw new NoSuchElementException();
            }
            Record buffer = peek;
            updatePeek();
            return buffer;
        }
    }
}