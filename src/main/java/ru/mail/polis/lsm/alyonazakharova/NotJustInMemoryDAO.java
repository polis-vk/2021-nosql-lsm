package ru.mail.polis.lsm.alyonazakharova;

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

public class NotJustInMemoryDAO implements DAO {

    private static final Method CLEAN;

    static {
        try {
            Class<?> aClass = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = aClass.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new IllegalStateException();
        }
    }

    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    @SuppressWarnings("unused")
    private final DAOConfig config;

    private final Path saveFileName;
    private final Path tmpFileName;
    private final MappedByteBuffer mmap;

    /**
     * Restore data if the file exists.
     *
     * @param config is used to get the file directory
     * @throws IOException if an I/O error occurs while opening FileChannel
     */
    public NotJustInMemoryDAO(DAOConfig config) throws IOException {
        this.config = config;

        saveFileName = config.getDir().resolve("save.dat");
        tmpFileName = config.getDir().resolve("tmp.dat");

        if (!Files.exists(saveFileName)) {
            if (Files.exists(tmpFileName)) {
                Files.move(tmpFileName, saveFileName, StandardCopyOption.ATOMIC_MOVE);
            } else {
                mmap = null;
                return;
            }
        }

        try (FileChannel channel = FileChannel.open(saveFileName, StandardOpenOption.READ)) {
            mmap = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
            while (mmap.hasRemaining()) {
                int keySize = mmap.getInt();
                ByteBuffer key = mmap.slice().limit(keySize).asReadOnlyBuffer();
                mmap.position(mmap.position() + keySize);
                int valueSize = mmap.getInt();
                ByteBuffer value = mmap.slice().limit(valueSize).asReadOnlyBuffer();
                mmap.position(mmap.position() + valueSize);
                storage.put(key, Record.of(key, value));
            }
        }
    }

    @Override
    public Iterator<Record> range(@Nullable final ByteBuffer fromKey,
                                  @Nullable final ByteBuffer toKey) {
        return map(fromKey, toKey).values().iterator();
    }

    @Override
    public void upsert(final Record record) {
        if (record.getValue() == null) {
            storage.remove(record.getKey());
        } else {
            storage.put(record.getKey(), record);
        }
    }

    @Override
    public void close() throws IOException {
        try (FileChannel fileChannel = FileChannel.open(tmpFileName, StandardOpenOption.WRITE,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

            final ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);

            for (final Record record : storage.values()) {
                writeValue(record.getKey(), fileChannel, size);
                writeValue(record.getValue(), fileChannel, size);
            }
        }

        if (mmap != null) {
            try {
                CLEAN.invoke(null, mmap);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException(e);
            }
        }

        Files.deleteIfExists(saveFileName);
        Files.move(tmpFileName, saveFileName, StandardCopyOption.ATOMIC_MOVE);
    }

    private SortedMap<ByteBuffer, Record> map(@Nullable final ByteBuffer fromKey,
                                              @Nullable final ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return storage;
        } else if (fromKey == null) {
            return storage.headMap(toKey);
        } else if (toKey == null) {
            return storage.tailMap(fromKey);
        }
        return storage.subMap(fromKey, toKey);
    }

    private static void writeValue(ByteBuffer value,
                                   WritableByteChannel channel,
                                   ByteBuffer size) throws IOException {
        size.position(0);
        size.putInt(value.remaining());
        size.position(0);
        channel.write(size);
        channel.write(value);
    }
}
