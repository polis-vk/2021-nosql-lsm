package ru.mail.polis.lsm;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Minimal database API.
 */
public interface DAO extends Closeable {
    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey);

    void upsert(Record record);

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

    class Entry {
        Iterator<Record> iterator;
        Record prevRecord;
        int order;

        /**
         * Util class to store current iterator state.
         *
         * @param iterator   the iterator
         * @param prevRecord value of the iterator
         * @param order      order of an iterator
         */
        private Entry(Iterator<Record> iterator, Record prevRecord, int order) {
            this.iterator = iterator;
            this.prevRecord = prevRecord;
            this.order = order;
        }
    }

    /**
     * Do merge iterators into one iterator.
     */
    static Iterator<Record> merge(List<Iterator<Record>> iterators) {
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

        List<Record> returnList = new ArrayList<>();
        while (!queue.isEmpty()) {
            Entry poll = queue.poll();
            clearQueue(queue, poll);

            returnList.add(poll.prevRecord);
            if (poll.iterator.hasNext()) {
                poll.prevRecord = poll.iterator.next();
                queue.add(poll);
            }
        }

        return returnList.subList(0, returnList.size()).iterator();
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
