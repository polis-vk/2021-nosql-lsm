package ru.mail.polis.lsm.shabinsky;

import ru.mail.polis.lsm.Record;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.stream.IntStream;

/**
 * MergeIterator.
 */
public class MergeRecordIterator implements Iterator<Record> {

    private final Queue<PeekRecordIterator> queue = new PriorityQueue<>();

    /**
     * MergeIterator constructor.
     *
     * @param iterators iterators
     */
    public MergeRecordIterator(List<Iterator<Record>> iterators) {
        IntStream
            .range(0, iterators.size())
            .filter(i -> iterators.get(i).hasNext())
            .forEach(i -> queue.offer(new PeekRecordIterator(iterators.get(i), i)));
    }

    @Override
    public boolean hasNext() {
        if (queue.isEmpty()) return false;
        for (PeekRecordIterator iterator : queue) {
            if (!iterator.hasNext()) {
                return false;
            }
            if (iterator.peek().isTombstone() && !iterator.hasDoubleNext()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Record next() {
        PeekRecordIterator peekIt = queue.remove();
        PeekRecordIterator peekItFind = queue.poll();

        while (peekItFind != null) {
            ByteBuffer currentKey = peekIt.peek().getKey();
            ByteBuffer findKey = peekItFind.peek().getKey();

            if (!currentKey.equals(findKey)) {
                break;
            }

            if (peekItFind.hasDoubleNext()) {
                peekItFind.next();
                queue.offer(peekItFind);
            }
            peekItFind = queue.poll();
        }
        if (peekItFind != null) queue.offer(peekItFind);

        Record returnNext = peekIt.next();
        if (peekIt.hasNext()) {
            queue.offer(peekIt);
        }
        return returnNext;
    }
}
