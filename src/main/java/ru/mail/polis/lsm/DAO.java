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

    static Iterator<Record> merge(List<Iterator<Record>> iterators) {

        class QueueUnit implements Comparable<QueueUnit> {
            private final Record data;
            private final int source;

            public QueueUnit(Record data, int source) {
                this.data = data;
                this.source = source;
            }


            public Record getData() {
                return data;
            }

            public int getSourceNumber() {
                return source;
            }

            @Override
            public int compareTo(QueueUnit o) {
                int keyCompare = data.getKey().compareTo(o.getData().getKey());
                if (keyCompare == 0) {
                    return o.getSourceNumber() - source;
                }
                return keyCompare;
            }
        }

        PriorityQueue<QueueUnit> queue = new PriorityQueue<>();

        for (int iterNumber = 0; iterNumber < iterators.size(); iterNumber++) {
            Iterator<Record> iterator = iterators.get(iterNumber);
            if (iterator.hasNext()) {
                queue.add(new QueueUnit(iterator.next(), iterNumber));
            }
        }

        ArrayList<Record> result = new ArrayList<Record>();

        while (!queue.isEmpty()) {
            QueueUnit current = queue.poll();


            Iterator<Record> currentIter = iterators.get(current.getSourceNumber());
            if (result.isEmpty() || !current.getData().getKey().equals(result.get(result.size() - 1).getKey())) {
                result.add(current.getData());
            }

            if (currentIter.hasNext()) {
                queue.add(new QueueUnit(currentIter.next(), current.getSourceNumber()));
            }

        }

        return result.iterator();
    }

}
