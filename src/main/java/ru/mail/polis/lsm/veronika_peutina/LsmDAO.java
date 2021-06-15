package ru.mail.polis.lsm.veronika_peutina;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;


public class LsmDAO implements DAO {

    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    private int memoryConsumption;
    private static final int MEMORY_LIMIT = 1024 * 1024 * 32;
    private final ConcurrentLinkedDeque<SSTable> ssTables = new ConcurrentLinkedDeque<>();
    private final Path dir;

    public LsmDAO(DAOConfig config) throws IOException {
        dir = config.getDir();
        memoryConsumption = 0;
        ssTables.addAll(SSTable.loadFromDir(config.getDir()));
    }

    private Path getNewName(Path dir) {
        String name = "SSTable" + ssTables.size() + ".dat";
        return dir.resolve(name);
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        synchronized (this) {
            Iterator<Record> ssTableRanges = getSSTableRanges(fromKey, toKey);
            Iterator<Record> memoryRange = SSTable.getSubMap(storage, fromKey, toKey).values().iterator();
            return merge(List.of(ssTableRanges, memoryRange));
        }
    }

    private Iterator<Record> getSSTableRanges(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>();
        ssTables.forEach(it -> iterators.add(it.range(storage, fromKey, toKey)));
        return merge(iterators);
    }

    @Override
    public void upsert(Record record) throws UncheckedIOException {
        synchronized (this) {
            memoryConsumption += sizeOfRecord(record);
            if (memoryConsumption >= MEMORY_LIMIT) {
                try {
                    flush();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }

        if (record.getKey() != null) {
            if (record.getValue() != null) {
                storage.put(record.getKey(), record);
            } else {
                storage.remove(record.getKey());
            }
        }
    }

    private int sizeOfRecord(Record record) {
        ByteBuffer key = record.getKey();
        ByteBuffer value = record.getValue();

        if (key == null) {
            if (value == null) {
                return 0;
            }
            return value.capacity();
        }

        if (value == null) {
            return key.capacity();
        }

        return key.capacity() + value.capacity();
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            flush();

            for (SSTable it : ssTables) {
                it.close();
            }
        }
    }

    @GuardedBy("this")
    private void flush() throws IOException {
        if (memoryConsumption == 0) {
            return;
        }
        ssTables.add(SSTable.write(storage.values().iterator(), getNewName(dir)));
        storage.clear();
        memoryConsumption = 0;
    }

    private static Iterator<Record> mergeTwoIterators(Iterator<Record> left, Iterator<Record> right) {

        return new Iterator<>() {
            Record leftRecord;
            Record rightRecord;

            @Override
            public boolean hasNext() {
                return left.hasNext() || right.hasNext();
            }

            @Override
            public Record next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("No elements");
                }
                leftRecord = getNext(left, leftRecord);
                rightRecord = getNext(right, rightRecord);
                if (leftRecord == null) {
                    return consumeRight();
                }
                if (rightRecord == null) {
                    return consumeLeft();
                }
                int comp = leftRecord.getKey().compareTo(rightRecord.getKey());
                if (comp == 0) {
                    leftRecord = null;
                    return consumeRight();
                }
                if (comp < 0) {
                    return consumeLeft();
                } else {
                    return consumeRight();
                }
            }

            private Record consumeLeft() {
                Record tmp = this.leftRecord;
                leftRecord = null;
                return tmp;
            }

            private Record consumeRight() {
                Record tmp = this.rightRecord;
                rightRecord = null;
                return tmp;
            }

            private Record getNext(Iterator<Record> iterator, @Nullable Record current) {
                if (current != null) {
                    return current;
                }
                if (iterator.hasNext()) {
                    return iterator.next();
                }
                return null;
            }
        };
    }

    public static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        if (iterators.size() == 0) {
            return Collections.emptyIterator();
        }
        if (iterators.size() == 1) {
            return iterators.get(0);
        }
        if (iterators.size() == 2) {
            return mergeTwoIterators(iterators.get(0), iterators.get(1));
        }
        Iterator<Record> left = merge(iterators.subList(0, iterators.size() / 2));
        Iterator<Record> right = merge(iterators.subList(iterators.size() / 2, iterators.size()));
        return mergeTwoIterators(left, right);
    }
}