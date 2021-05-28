package ru.mail.polis.lsm.igorsamokhin;

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
import java.nio.channels.ReadableByteChannel;
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
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    private final DAOConfig config;
    private final MappedByteBuffer map;

    private final Path filePath;
    private final Path tmpFilePath;

    /**
     * Create DAO object.
     *
     * @param config - objects contains directory with data files
     */
    public NotJustInMemoryDAO(DAOConfig config) throws IOException {
        this.config = config;

        Path dir = this.config.getDir();
        filePath = dir.resolve("save.dat");
        tmpFilePath = dir.resolve("tmp.dat");
        if (!Files.exists(filePath)) {
            if (Files.exists(tmpFilePath)) {
                Files.move(tmpFilePath, filePath, StandardCopyOption.ATOMIC_MOVE);
            } else {
                map = null;
                return;
            }
        }

        try (FileChannel fileChannel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            map = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());

            while (map.hasRemaining()) {
                ByteBuffer key = readValue(map);
                ByteBuffer value = readValue(map);

                storage.put(key, Record.of(key, value));
            }
        }
    }

    private ByteBuffer readValue(MappedByteBuffer map) {
        int size = map.getInt();
        ByteBuffer value = map.slice().limit(size).asReadOnlyBuffer();
        map.position(map.position() + size);
        return value;
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return getSubMap(fromKey, toKey).values().iterator();
    }

    private SortedMap<ByteBuffer, Record> getSubMap(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return storage;
        } else if (fromKey == null) {
            return storage.headMap(toKey);
        } else if (toKey == null) {
            return storage.tailMap(fromKey);
        }
        return storage.subMap(fromKey, toKey);
    }

    @Override
    public void upsert(Record record) {
        if (record.getValue() == null) {
            storage.remove(record.getKey());
        } else {
            storage.put(record.getKey(), record);
        }
    }

    @Override
    public void close() throws IOException {
        Files.deleteIfExists(tmpFilePath);
        Files.createDirectories(config.getDir());

        try (FileChannel fileChannel = FileChannel.open(
                tmpFilePath,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE_NEW
        )) {
            final ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            for (final Record record : storage.values()) {
                writeInt(record.getKey(), fileChannel, size);
                writeInt(record.getValue(), fileChannel, size);
            }
            fileChannel.force(false);
        }

        if (map != null) {
            try {
                CLEAN.invoke(null, map);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IOException(e);
            }
        }

        Files.deleteIfExists(filePath);
        Files.move(tmpFilePath, filePath, StandardCopyOption.ATOMIC_MOVE);
    }

    private void writeInt(ByteBuffer value, WritableByteChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        tmp.putInt(value.remaining());
        tmp.position(0);
        channel.write(tmp);
        channel.write(value);
    }
}
