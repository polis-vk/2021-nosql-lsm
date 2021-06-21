package ru.mail.polis.lsm.shabinsky;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;

public class LmsDAO implements DAO {

    private final SortedMap<ByteBuffer, Record> memoryStorage = new ConcurrentSkipListMap<>();
    private final ConcurrentLinkedDeque<SSTable> tables = new ConcurrentLinkedDeque<>();

    private final DAOConfig config;

    private static final int MEM_LIM = 20 * 1024 * 1024;
    private int memorySize;

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
            Iterator<Record> sstableRanges = sstableRanges(fromKey, toKey);
            Iterator<Record> memoryRange = map(fromKey, toKey).values().iterator();
            Iterator<Record> iterator = mergeTwo(new PeekingIterator(sstableRanges), new PeekingIterator(memoryRange));
            return filterTombstones(iterator);
        }
    }

    @Override
    public void upsert(Record record) {
        synchronized (this) {
            memorySize += record.sizeOf();
            if (memorySize >= MEM_LIM) {
                try {
                    flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                memorySize += record.sizeOf();
            }

            memoryStorage.put(record.getKey(), record);
        }
    }

    @Override
    public void compact() throws IOException {
        synchronized (this) {
            if (!memoryStorage.isEmpty()) flush();

            Iterator<Record> records = range(null, null);
            List<SSTable> newTables = SSTable.compact(config.getDir(), records, MEM_LIM);

            for (SSTable ssTable : tables) ssTable.close();

            SSTable.cleanForComp(config.getDir());

            tables.clear();
            tables.addAll(newTables);
            this.genIndex = tables.size();
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

    private Iterator<Record> sstableRanges(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(tables.size());
        for (SSTable ssTable : tables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return merge(iterators);
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

    private static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        if (iterators.isEmpty()) {
            return Collections.emptyIterator();
        }
        if (iterators.size() == 1) {
            return iterators.get(0);
        }
        if (iterators.size() == 2) {
            return mergeTwo(new PeekingIterator(iterators.get(0)), new PeekingIterator(iterators.get(1)));
        }
        Iterator<Record> left = merge(iterators.subList(0, iterators.size() / 2));
        Iterator<Record> right = merge(iterators.subList(iterators.size() / 2, iterators.size()));
        return mergeTwo(new PeekingIterator(left), new PeekingIterator(right));
    }

    private static Iterator<Record> mergeTwo(PeekingIterator left, PeekingIterator right) {
        return new Iterator<>() {

            @Override
            public boolean hasNext() {
                return left.hasNext() || right.hasNext();
            }

            @Override
            public Record next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("No elements");
                }

                if (!left.hasNext()) {
                    return right.next();
                }
                if (!right.hasNext()) {
                    return left.next();
                }

                // checked earlier
                ByteBuffer leftKey = Objects.requireNonNull(left.peek()).getKey();
                ByteBuffer rightKey = Objects.requireNonNull(right.peek()).getKey();

                int compareResult = leftKey.compareTo(rightKey);
                if (compareResult == 0) {
                    left.next();
                    return right.next();
                }

                if (compareResult < 0) {
                    return left.next();
                } else {
                    return right.next();
                }
            }

        };
    }

    private static Iterator<Record> filterTombstones(Iterator<Record> iterator) {
        PeekingIterator delegate = new PeekingIterator(iterator);
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                for (; ; ) {
                    Record peek = delegate.peek();
                    if (peek == null) {
                        return false;
                    }
                    if (!peek.isTombstone()) {
                        return true;
                    }

                    delegate.next();
                }
            }

            @Override
            public Record next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("No elements");
                }
                return delegate.next();
            }
        };
    }

    private static class PeekingIterator implements Iterator<Record> {

        private Record current;

        private final Iterator<Record> delegate;

        public PeekingIterator(Iterator<Record> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return current != null || delegate.hasNext();
        }

        @Override
        public Record next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Record now = peek();
            current = null;
            return now;
        }

        public Record peek() {
            if (current != null) {
                return current;
            }

            if (!delegate.hasNext()) {
                return null;
            }

            current = delegate.next();
            return current;
        }

    }
}
