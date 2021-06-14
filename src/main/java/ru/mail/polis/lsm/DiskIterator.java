package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class DiskIterator<T> implements Iterator<Record> {
    private final ByteBuffer fromKey;
    private final ByteBuffer toKey;
    private Record current = null;
    private final ByteBuffer buffer;

    /**
     * Iterator from hard drive.
     */
    public DiskIterator(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey, ByteBuffer buffer) {
        this.fromKey = fromKey;
        this.toKey = toKey;
        this.buffer = buffer;
        toStartPosition();
        getCurrent();
    }

    private void getCurrent() {
        if (!buffer.hasRemaining() || (toKey != null && current.getKey().compareTo(toKey) > 0)) {
            current = null;
            return;
        }
        int keySize = buffer.getInt();
        ByteBuffer key = buffer.slice().limit(keySize).asReadOnlyBuffer();

        buffer.position(buffer.position() + keySize);

        int valueSize = buffer.getInt();
        if (valueSize < 0) {
            current = Record.tombstone(key);

        } else {
            ByteBuffer value = buffer.slice().limit(valueSize).asReadOnlyBuffer();

            buffer.position(buffer.position() + valueSize);

            current = Record.of(key, value);
        }

    }

    private void toStartPosition() {
        if (fromKey == null) {
            buffer.position(0);
            return;
        }

        ByteBuffer key = null;
        int keySize = 0;
        int valueSize = 0;
        if (buffer.hasRemaining()) {
            do {
                keySize = buffer.getInt();
                key = buffer.slice().limit(keySize).asReadOnlyBuffer();
                buffer.position(buffer.position() + keySize);

                valueSize = buffer.getInt();
                if (valueSize < 0) {
                    valueSize = 0;
                }
                buffer.position(buffer.position() + valueSize);
            } while (buffer.hasRemaining() && key.compareTo(fromKey) < 0);
        }
        if (key.compareTo(fromKey) >= 0) {
            buffer.position(buffer.position() - keySize - valueSize - Integer.BYTES * 2);
        }
        current = Record.tombstone(key);
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
}
