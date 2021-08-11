package ru.mail.polis.lsm.saveliyschur.sstservice;

import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class SSTable implements ISSTable {

    private static final Method CLEAN;
    public static final String SSTABLE_FILE_PREFIX = "file_";
    private final Path file;

    static {
        try {
            Class<?> aClass = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = aClass.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private final MappedByteBuffer mmap;
    private final MappedByteBuffer idx;

    public SSTable(Path file) throws IOException {
        this.file = file;

        Path indexFile = getIndexFile(file);

        mmap = open(file);
        idx = open(indexFile);
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        ByteBuffer buffer = mmap.asReadOnlyBuffer();

        int maxSize = mmap.remaining();

        int fromOffset = fromKey == null ? 0 : offset(buffer, fromKey);
        int toOffset = toKey == null ? maxSize : offset(buffer, toKey);

        return range(
                buffer,
                fromOffset == -1 ? maxSize : fromOffset,
                toOffset == -1 ? maxSize : toOffset
        );
    }

    @Override
    public void close() throws IOException {
        IOException exception = null;
        try {
            free(mmap);
        } catch (Throwable t) {
            exception = new IOException(t);
        }

        try {
            free(idx);
        } catch (Throwable t) {
            if (exception == null) {
                exception = new IOException(t);
            } else {
                exception.addSuppressed(t);
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    @Override
    public void deleteWithClose() {
        try {
            this.close();
            Files.deleteIfExists(file);
            Files.deleteIfExists(getIndexFile(file));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path resolveWithExt(Path file, String ext) {
        return file.resolveSibling(file.getFileName() + ext);
    }

    private Path getIndexFile(Path file) {
        return resolveWithExt(file, ".idx");
    }

    private static MappedByteBuffer open(Path name) throws IOException {
        try (
                FileChannel channel = FileChannel.open(name, StandardOpenOption.READ)
        ) {
            return channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        }
    }

    private static void free(MappedByteBuffer buffer) throws IOException {
        try {
            CLEAN.invoke(null, buffer);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IOException(e);
        }
    }

    private int offset(ByteBuffer buffer, ByteBuffer key) {
        int left = 0;
        int rightLimit = idx.remaining() / Integer.BYTES;
        int right = rightLimit;

        while (left < right) {
            int mid = left + ((right - left) >>> 1);

            int offset = idx.getInt(mid * Integer.BYTES);
            buffer.position(offset);
            int keySize = buffer.getInt();

            int result;
            int mismatch = buffer.mismatch(key);
            if (mismatch == -1) {
                return offset;
            } else if (mismatch < keySize) {
                result = Byte.compare(
                        key.get(key.position() + mismatch),
                        buffer.get(buffer.position() + mismatch)
                );
            } else {
                result = -1;
            }

            if (result > 0) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        if (left >= rightLimit) {
            return -1;
        }

        return idx.getInt(left * Integer.BYTES);
    }

    private static Iterator<Record> range(ByteBuffer buffer, int fromOffset, int toOffset) {
        buffer.position(fromOffset);

        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return buffer.position() < toOffset;
            }

            @Override
            public Record next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("Limit is reached");
                }

                int keySize = buffer.getInt();
                ByteBuffer key = read(keySize);

                int valueSize = buffer.getInt();
                if (valueSize == -1) {
                    return Record.tombstone(key);
                }
                ByteBuffer value = read(valueSize);

                return Record.of(key, value);
            }

            private ByteBuffer read(int size) {
                ByteBuffer result = (ByteBuffer) buffer.slice().limit(size);
                buffer.position(buffer.position() + size);
                return result;
            }
        };
    }
}
