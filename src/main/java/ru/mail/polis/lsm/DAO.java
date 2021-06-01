package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Minimal database API.
 */
public interface DAO extends Closeable {
    /**
     * Appends {@code Byte.MIN_VALUE} to {@code buffer}.
     *
     * @param buffer original {@link ByteBuffer}
     * @return copy of {@code buffer} with {@code Byte.MIN_VALUE} appended
     */
    static ByteBuffer nextKey(ByteBuffer buffer) {
        ByteBuffer result = ByteBuffer.allocate(buffer.remaining() + 1);

        int position = buffer.position();

        result.put(buffer);
        result.put(Byte.MIN_VALUE);

        buffer.position(position);
        result.rewind();

        return result;
    }

    /**
     *
     * @param iterators
     * @return
     */

    static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        if (iterators.isEmpty()) {
            return Collections.emptyIterator();
        }
        if (iterators.size() == 1) {
            return iterators.get(0);
        }
        return new MyIterator(iterators);
    }

    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey);

    void upsert(Record record);

    class Pair {

        Record record;
        Iterator<Record> iterator;

        Pair(Record record, Iterator<Record> iterator) {
            this.record = record;
            this.iterator = iterator;
        }
    }

    class MyIterator implements Iterator<Record> {

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
            for(Pair pair : nextRecordList) {
                if(pair.iterator != null) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Record next() {
            if(!hasNext()) {
                throw new NoSuchElementException();
            }
            Record minRecord = null;
            int minIndex = -1;
            for(int i = 0; i < nextRecordList.size(); ++i) {
                Record currentRecord = nextRecordList.get(i).record;
                if(currentRecord != null) {
                    if(minRecord == null || minRecord.getKey().compareTo(currentRecord.getKey()) >= 0) {
                        minRecord = currentRecord;
                        minIndex = i;
                    }
                }
            }
            Iterator<Record> iteratorToUpdate = nextRecordList.get(minIndex).iterator;
            if(iteratorToUpdate != null && minIndex != -1) {
                if(iteratorToUpdate.hasNext()) {
                    nextRecordList.set(minIndex, new Pair(iteratorToUpdate.next(), iteratorToUpdate));
                } else {
                    nextRecordList.set(minIndex, new Pair(null, null));
                }
            }

            for(int i = 0; i < nextRecordList.size(); ++i) {
                if(i == minIndex) {
                    continue;
                }
                Pair pair = nextRecordList.get(i);
                while(pair.iterator != null && pair.record.getKey().equals(minRecord.getKey())) {
                    if(pair.iterator.hasNext()) {
                        pair.record = pair.iterator.next();
                    } else {
                        pair.iterator = null;
                        pair.record = null;
                    }
                }
            }

            return minRecord;
        }
    }
}
