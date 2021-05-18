package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

@SuppressWarnings("JavaLangClash")
public class Record {

    private final ByteBuffer key;
    private final ByteBuffer value;

    Record(ByteBuffer key, @Nullable ByteBuffer value) {
        this.key = key;
        this.value = value;
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
}
