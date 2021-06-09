package ru.mail.polis.lsm.ilia;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class DAOImpl implements DAO {

    private final SortedMap<ByteBuffer, Record> memoryStorage = new ConcurrentSkipListMap<>();

    private final DAOConfig config;
    private final ArrayList<SSTable> tables = new ArrayList<>();

    /**
     * Create DAOImpl constructor.
     *
     * @param config contains directory with file
     */
    public DAOImpl(DAOConfig config) throws IOException {
        this.config = config;
        tables.add(SSTable.loadFromDir(config.getDir()));
    }

    private static void writeInt(ByteBuffer value, WritableByteChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        tmp.putInt(value.remaining());
        tmp.position(0);
        channel.write(tmp);
        channel.write(value);
    }

    /**
     * Merges iterators.
     *
     * @param iterators iterators List
     * @return result Iterator
     */
    private static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        switch (iterators.size()) {
            case (0):
                return Collections.emptyIterator();
            case (1):
                return iterators.get(0);
            case (2):
                return new MergeTwo(new PeekingIterator(iterators.get(0)), new PeekingIterator(iterators.get(1)));
            default:
                Iterator<Record> left = merge(iterators.subList(0, iterators.size() / 2));
                Iterator<Record> right = merge(iterators.subList(iterators.size() / 2, iterators.size()));

                return new MergeTwo(new PeekingIterator(left), new PeekingIterator(right));
        }
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        synchronized (this) {
            Iterator<Record> sstableRanges = sstableRanger(fromKey, toKey);
            Iterator<Record> memoryRange = map(fromKey, toKey).values().iterator();
            return new MergeTwo(sstableRanges, memoryRange);
        }
    }

    private Iterator<Record> sstableRanger(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(tables.size());
        for (SSTable ssTable : tables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return merge(iterators);
    }

    @Override
    public void upsert(Record record) {
        synchronized (this) {
            memoryCompsumption += sizeOf(record);
            if (memoryCompsumption > memoryLimit) {
                try {
                    flush();
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

    private void flush() throws IOException {
        SSTable ssTable = SSTable.write(memoryStorage.values().iterator(), ...);
        tables.add(ssTable);
        memoryStorage.clear();
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

    static class MergeTwo implements Iterator<Record> {
        private final PeekingIterator left;
        private final PeekingIterator right;

        private MergeTwo(PeekingIterator left, PeekingIterator right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public boolean hasNext() {
            return left.hasNext() || right.hasNext();
        }

        @Override
        public Record next() {
            if (!left.hasNext() && !right.hasNext()) {
                throw new NoSuchElementException();
            }

            if (!left.hasNext()) {
                return right.next();
            }

            if (!right.hasNext()) {
                return left.next();
            }

            ByteBuffer leftKey = left.peek().getKey();
            ByteBuffer rightKey = right.peek().getKey();

            int compareResult = leftKey.compareTo(rightKey);
            if (compareResult == 0) {
                left.next();
                return right.next();
            } else if (compareResult > 0) {
                return right.next();
            } else {
                return left.next();
            }
        }
    }

    private static class PeekingIterator implements Iterator<Record> {
        private final Iterator<Record> delegate;
        private Record current;

        public PeekingIterator(Iterator<Record> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return current != null || delegate.hasNext();
        }

        @Override
        public Record next() {
            if ((!hasNext())) {
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

            if (!hasNext()) {
                return null;
            }

            current = delegate.next();
            return current;
        }
    }


}
