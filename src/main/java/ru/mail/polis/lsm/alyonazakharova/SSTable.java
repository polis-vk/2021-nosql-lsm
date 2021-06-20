package ru.mail.polis.lsm.alyonazakharova;

import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class SSTable implements Closeable {

    private static final Method CLEAN;
    private static final String SSTABLE_FILE_PREFIX = "file_";

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
        Path indexFile = getIndexFile(file);
        mmap = open(file);
        idx = open(indexFile);
    }

    public static List<SSTable> loadFromDir(Path dir) throws IOException {
        List<SSTable> ssTables = new ArrayList<>();
        for (int i = 0; ; i++) {
            Path file = dir.resolve(SSTABLE_FILE_PREFIX + i);
            if (!Files.exists(file)) {
                return ssTables;
            }
            ssTables.add(new SSTable(file));
        }
    }

    public static SSTable write(Path file, Iterator<Record> iterator) throws IOException {
        Path tmpFile = getTmpFile(file);
        Path indexFile = getIndexFile(file);
        Path tmpIndexFile = getTmpFile(indexFile);

        try (FileChannel fileChannel = openForWrite(tmpFile);
             FileChannel indexChannel = openForWrite(tmpIndexFile)) {

            final ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);

            while (iterator.hasNext()) {
                long position = fileChannel.position();
                if (position > Integer.MAX_VALUE) {
                    throw new IllegalStateException("File is too long");
                }
                writeInt((int) position, indexChannel, size);

                Record record = iterator.next();
                writeValueWithSize(record.getKey(), fileChannel, size);
                if (record.isTombstone()) {
                    writeInt(-1, fileChannel, size);
                } else {
                    ByteBuffer value = Objects.requireNonNull(record.getValue());
                    writeValueWithSize(value, fileChannel, size);
                }
            }
            fileChannel.force(false);
        }

        rename(indexFile, tmpIndexFile);
        rename(file, tmpFile);

        return new SSTable(file);
    }

    public static int sizeOf(Record record) {
        int keySize = Integer.BYTES + record.getKeySize();
        int valueSize = Integer.BYTES + record.getValueSize();
        return keySize + valueSize;
    }

    public Iterator<Record> range(@Nullable final ByteBuffer fromKey, @Nullable final ByteBuffer toKey) {
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
        } catch (IOException e) {
            exception = e;
        } catch (Throwable t) {
            exception = new IOException(t);
        }
        try {
            free(idx);
        } catch (IOException e) {
            if (exception == null) {
                 exception = e;
            } else {
                exception.addSuppressed(e);
            }
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

    private static void free(MappedByteBuffer buffer) throws IOException {
        try {
            CLEAN.invoke(null, buffer);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IOException(e);
        }
    }

    private int offset(ByteBuffer buffer, final ByteBuffer key) {
        int left = 0;
        int rightLimit = idx.remaining() / Integer.BYTES;
        int right = rightLimit;

        while (left < right) {
            int mid = left + (right - left) >>> 1;
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
                ByteBuffer result = buffer.slice().limit(size);
                buffer.position(buffer.position() + size);
                return result;
            }
        };
    }

    private static Path getIndexFile(Path file) {
        return resolveWithExt(file, ".idx");
    }

    private static Path getTmpFile(Path file) {
        return resolveWithExt(file, ".tmp");
    }

    private static Path resolveWithExt(Path file, String ext) {
        return file.resolveSibling(file.getFileName() + ext);
    }

    private static MappedByteBuffer open(Path name) throws IOException {
        try (FileChannel channel = FileChannel.open(name, StandardOpenOption.READ)) {
            return channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        }
    }

    private static FileChannel openForWrite(Path tmpFile) throws IOException {
        return FileChannel.open(
                tmpFile,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
    }

    private static void rename(Path file, Path tmpFile) throws IOException {
        Files.deleteIfExists(file);
        Files.move(tmpFile, file, StandardCopyOption.ATOMIC_MOVE);
    }

    private static void writeValueWithSize(ByteBuffer value,
                                           WritableByteChannel channel,
                                           ByteBuffer size) throws IOException {
        writeInt(value.remaining(), channel, size);
        channel.write(size);
        channel.write(value);
    }

    private static void writeInt(int value,
                                 WritableByteChannel channel,
                                 ByteBuffer size) throws IOException {
        size.position(0);
        size.putInt(value);
        size.position(0);
        channel.write(size);
    }
}
