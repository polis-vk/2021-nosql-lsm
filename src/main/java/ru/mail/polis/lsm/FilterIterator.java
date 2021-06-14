package ru.mail.polis.lsm;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class FilterIterator implements Iterator<Record> {
    private final Iterator<Record> iter;
    private Record current;

    public FilterIterator(Iterator<Record> iterator) {
        this.iter = iterator;
        getCurrent();
    }

    @Override
    public boolean hasNext() {
        return current != null;
    }

    @Override
    public Record next() {
        Record result = current;
        getCurrent();
        return result;
    }

    private void getCurrent() {
        Record next = Record.tombstone(ByteBuffer.allocate(Integer.BYTES));
        while (next.isTombstone() && iter.hasNext()) {
            next = iter.next();
        }
        if (next.isTombstone()) {
            current = null;
        } else {
            current = next;
        }
    }
}
