package ru.mail.polis.lsm.danilafedorov;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
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
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

public class FileMemoryDAO implements DAO {

    private static final Method CLEAN;

    static {
        try {
            Class<?> clas = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = clas.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    private MappedByteBuffer mmap;

    private final Path dataFilePath;
    private final Path tempFilePath;

    private static final String DATA_FILE_NAME = "Data";
    private static final String TEMP_FILE_NAME = "Temp";

    /**
     * Class constructor identifying directory of DB location.
     *
     * @param config contains directory path of DB location
     * @throws IOException If an input exception occured
     */
    public FileMemoryDAO(final DAOConfig config) throws IOException {
        Path dir = config.getDir();
        dataFilePath = dir.resolve(DATA_FILE_NAME);
        tempFilePath = dir.resolve(TEMP_FILE_NAME);
        restore();
    }

    @Override
    public Iterator<Record> range(@Nullable final ByteBuffer fromKey, @Nullable final ByteBuffer toKey) {
        return getStreamOfValidValues(fromKey, toKey).iterator();
    }

    @Override
    public void upsert(final Record record) {
        storage.put(record.getKey(), record);
    }

    @Override
    public void close() throws IOException {
        save();
    }

    private SortedMap<ByteBuffer, Record> map(@Nullable final ByteBuffer fromKey, @Nullable final ByteBuffer toKey) {
        if (fromKey == null) {
            if (toKey == null) {
                return storage;
            }

            return storage.headMap(toKey);
        }

        if (toKey == null) {
            return storage.tailMap(fromKey);
        }

        return storage.subMap(fromKey, toKey);
    }

    private Stream<Record> getStreamOfValidValues(
            @Nullable final ByteBuffer fromKey,
            @Nullable final ByteBuffer toKey
    ) {
        return map(fromKey, toKey).values()
                .stream()
                .filter(record -> !record.isTombstone());
    }

    private void restore() throws IOException {
        if (!Files.exists(dataFilePath)) {
            if (Files.exists(tempFilePath)) {
                Files.move(tempFilePath, dataFilePath, StandardCopyOption.ATOMIC_MOVE);
            } else {
                mmap = null;
                return;
            }
        }

        try (FileChannel channel = FileChannel.open(dataFilePath, StandardOpenOption.READ)) {
            mmap = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
            while (mmap.hasRemaining()) {
                ByteBuffer key = read();
                ByteBuffer value = read();
                storage.put(key, Record.of(key, value));
            }
        }

    }

    private void save() throws IOException {
        try (FileChannel channel = FileChannel.open(
                tempFilePath,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {
            final ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            for (final Record record : storage.values()) {
                if (!record.isTombstone()) {
                    write(channel, record.getKey(), size);
                    write(channel, record.getValue(), size);
                }
            }
            channel.force(false);
        }

        if (mmap != null) {
            try {
                CLEAN.invoke(null, mmap);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IOException(e);
            }
        }

        Files.deleteIfExists(dataFilePath);
        Files.move(tempFilePath, dataFilePath, StandardCopyOption.ATOMIC_MOVE);
    }

    private void write(WritableByteChannel channel, ByteBuffer value, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        tmp.putInt(value.remaining());
        tmp.position(0);
        channel.write(tmp);
        channel.write(value);
    }

    private ByteBuffer read() {
        int size = mmap.getInt();
        ByteBuffer value = mmap.slice().limit(size).asReadOnlyBuffer();
        mmap.position(mmap.position() + size);
        return value;
    }
}
