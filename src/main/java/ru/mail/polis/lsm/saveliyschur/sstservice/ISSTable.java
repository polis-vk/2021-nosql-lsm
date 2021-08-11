package ru.mail.polis.lsm.saveliyschur.sstservice;

import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Iterator;

public interface ISSTable extends Closeable {
    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey);
    void deleteWithClose();
}
