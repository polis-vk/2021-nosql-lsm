package ru.mail.polis.lsm.sachuk.ilya;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;

public class DaoImpl implements DAO {

    private final DAOConfig config;
    private final SortedMap<ByteBuffer, Record> memoryStorage = new ConcurrentSkipListMap<>();
    private final ConcurrentLinkedDeque<SSTable> ssTables = new ConcurrentLinkedDeque<>();

    private long memoryConsumption = 0;

    /**
     * Constructor that initialize path and restore storage.
     *
     * @param config is config.
     * @throws IOException is thrown when an I/O error occurs.
     */
    public DaoImpl(DAOConfig config) throws IOException {
        this.config = config;

        ssTables.addAll(SSTable.loadFromDir(config.getDir()));
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        synchronized (this) {
            Iterator<Record> sstableRanges = sstableRanges(fromKey, toKey);
            Iterator<Record> memoryRange = map(fromKey, toKey).values().iterator();

            return mergeTwo(sstableRanges, memoryRange);
        }
    }

    @Override
    public void upsert(Record record) {

        synchronized (this) {
            memoryConsumption += 4;
            if (memoryConsumption > Integer.MAX_VALUE / 32) {
                try {
                    flush();
                    memoryConsumption = 0;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
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

    private Map<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {

        if (fromKey == null && toKey == null) {
            return memoryStorage;
        } else if (fromKey == null) {
            return memoryStorage.headMap(toKey);
        } else if (toKey == null) {
            return memoryStorage.tailMap(fromKey);
        } else {
            return memoryStorage.subMap(fromKey, toKey);
        }
    }

    private void flush() throws IOException {
        SSTable ssTable = SSTable.save(memoryStorage.values().iterator(), config.getDir());
        ssTables.add(ssTable);
        memoryStorage.clear();
    }

    private Iterator<Record> sstableRanges(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(ssTables.size());

        for (SSTable ssTable : ssTables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }

        return merge(iterators);
    }

    /**
     * Method that merge iterators and return iterator.
     *
     * @param iterators is list of iterators to merge
     * @return merged iterators
     */
    public static Iterator<Record> merge(List<Iterator<Record>> iterators) {

        if (iterators.isEmpty()) {
            return Collections.emptyIterator();
        } else if (iterators.size() == 1) {
            return iterators.get(0);
        } else if (iterators.size() == 2) {
            return mergeTwo(iterators.get(0), iterators.get(1));
        }

        Iterator<Record> left = merge(iterators.subList(0, iterators.size() / 2));
        Iterator<Record> right = merge(iterators.subList(iterators.size() / 2, iterators.size()));

        return mergeTwo(left, right);
    }

    private static Iterator<Record> mergeTwo(Iterator<Record> left, Iterator<Record> right) {
        return new Iterator<>() {

            private Record leftRecord;
            private Record rightRecord;

            @Override
            public boolean hasNext() {
                return left.hasNext() || right.hasNext();
            }

            @Override
            public Record next() {

                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                leftRecord = getNext(left, leftRecord);
                rightRecord = getNext(right, rightRecord);


                if (leftRecord == null) {
                    return getRight();
                }
                if (rightRecord == null) {
                    return getLeft();
                }

                int compare = leftRecord.getKey().compareTo(rightRecord.getKey());

                if (compare == 0) {
                    leftRecord = null;
                    return getRight();
                } else if (compare < 0) {
                    return getLeft();
                } else {
                    return getRight();
                }
            }

            private Record getRight() {
                Record tmp = rightRecord;
                rightRecord = null;

                return tmp;
            }

            private Record getLeft() {
                Record tmp = leftRecord;
                leftRecord = null;

                return tmp;
            }

            private Record getNext(Iterator<Record> iterator, @Nullable Record current) {
                if (current != null) {
                    return current;
                }

                return iterator.hasNext() ? iterator.next() : null;
            }
        };
    }
}
