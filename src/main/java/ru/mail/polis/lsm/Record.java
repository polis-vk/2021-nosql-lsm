package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

@SuppressWarnings("JavaLangClash")
public class Record {

    private final ByteBuffer key;
    private final ByteBuffer value;

    Record(ByteBuffer key, @Nullable ByteBuffer value) {
        this.key = key.asReadOnlyBuffer();
        this.value = value == null ? null : value.asReadOnlyBuffer();
    }


    public static Record of(ByteBuffer key, ByteBuffer value) {
        return new Record(key.asReadOnlyBuffer(), value.asReadOnlyBuffer());
    }

    public static Record tombstone(ByteBuffer key) {
        return new Record(key, null);
    }

    public ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    public ByteBuffer getValue() {
        return value == null ? null : value.asReadOnlyBuffer();
    }

    public boolean isTombstone() {
        return value == null;
    }

    // Можно перенести в конструктор?
    public int size() {
        byte sizePrefix = Integer.BYTES * 2;
        int sizeOfKey = sizeOf(key);
        int sizeOfValue = sizeOf(value);
        return sizePrefix + sizeOfKey + sizeOfValue;
    }

    private int sizeOf(ByteBuffer byteBuffer) {
        return byteBuffer.limit();
    }
}
