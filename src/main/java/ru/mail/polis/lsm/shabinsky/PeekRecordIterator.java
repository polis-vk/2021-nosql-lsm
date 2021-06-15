package ru.mail.polis.lsm.shabinsky;

import ru.mail.polis.lsm.Record;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * PeekIterator.
 */
public class PeekRecordIterator implements Iterator<Record>, Comparable<PeekRecordIterator> {

    private final Iterator<Record> delegate;
    private Record current;
    private final Integer number;

    public PeekRecordIterator(Iterator<Record> delegate, Integer number) {
        this.delegate = delegate;
        this.number = number;
    }

    public boolean hasDoubleNext() {
        return delegate.hasNext();
    }

    @Override
    public boolean hasNext() {
        if (current != null) return true;
        return delegate.hasNext();
    }

    @Override
    public Record next() {
        if (!hasNext()) throw new NoSuchElementException();

        Record now = peek();
        current = null;
        return now;
    }

    /**
     * Peek.
     *
     * @return Record
     */
    public Record peek() {
        if (current != null) return current;
        if (!delegate.hasNext()) return null;

        current = delegate.next();
        return current;
    }

    public Integer getNumber() {
        return number;
    }

    @Override
    public int compareTo(PeekRecordIterator o) {
        int compareKeyASC = this.peek().getKey().compareTo(o.peek().getKey());
        int compareNumberDESC = o.getNumber().compareTo(this.getNumber());
        return compareKeyASC == 0 ? compareNumberDESC : compareKeyASC;
    }
}
