package ru.mail.polis.lsm.segu.model;

import java.util.Objects;

public class IndexRecord {
    private final int index;
    private final int offset;

    public IndexRecord(int index, int offset) {
        this.index = index;
        this.offset = offset;
    }

    public int getIndex() {
        return index;
    }

    public int getOffset() {
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexRecord that = (IndexRecord) o;
        return index == that.index && offset == that.offset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, offset);
    }
}
