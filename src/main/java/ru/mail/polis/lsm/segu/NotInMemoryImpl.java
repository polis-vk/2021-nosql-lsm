package ru.mail.polis.lsm.segu;

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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Implementation of NotInMemory DAO.
 */

public class NotInMemoryImpl implements DAO {

    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    private MappedByteBuffer mappedByteBuffer;
    private static final Method CLEAN;
    private final DAOConfig config;
    private static final String SAVE_FILE_NAME = "data.dat";
    private Path saveFilePath;
    private static final String TMP_FILE_NAME = "temp.dat";
    private Path tmpFilePath;

    static {
        Class<?> aClass;
        try {
            aClass = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = aClass.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (NoSuchMethodException | ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }

    }

    /**
     * Constructor.
     *
     * @param config - конфиг
     */

    public NotInMemoryImpl(DAOConfig config) throws IOException {
        this.config = config;
        initStorage();
    }

    private void initStorage() throws IOException {
        saveFilePath = config.getDir().resolve(SAVE_FILE_NAME);
        tmpFilePath = config.getDir().resolve(TMP_FILE_NAME);
        if (!Files.exists(saveFilePath)) {
            if (Files.exists(tmpFilePath)) {
                Files.move(tmpFilePath, saveFilePath, StandardCopyOption.ATOMIC_MOVE);
            } else {
                mappedByteBuffer = null;
                return;
            }
        }
        try (FileChannel fileChannel = FileChannel.open(saveFilePath, StandardOpenOption.READ)) {
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
            while (mappedByteBuffer.hasRemaining()) {
                int keySize = mappedByteBuffer.getInt();
                ByteBuffer key = mappedByteBuffer.slice().limit(keySize).asReadOnlyBuffer();

                mappedByteBuffer.position(mappedByteBuffer.position() + keySize);

                int valueSize = mappedByteBuffer.getInt();

                if (valueSize < 0) {
                    storage.put(key, Record.tombstone(key));
                } else {
                    ByteBuffer value = mappedByteBuffer.slice().limit(valueSize).asReadOnlyBuffer();
                    storage.put(key, Record.of(key, value));
                }
                if (mappedByteBuffer.hasRemaining()) {
                    mappedByteBuffer.position(mappedByteBuffer.position() + valueSize);
                }
            }
        }
    }

    @Override
    public Iterator<Record> range(@Nullable final ByteBuffer fromKey, @Nullable final ByteBuffer toKey) {
        return map(fromKey, toKey).values().stream()
                .filter(record -> record.getValue() != null)
                .iterator();
    }

    @Override
    public void upsert(final Record record) {
        storage.put(record.getKey(), record);
    }

    @Override
    public void close() throws IOException {

        try (FileChannel fileChannel = FileChannel.open(tmpFilePath,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE)) {
            final ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            for (final Record record : storage.values()) {
                writeValue(fileChannel, record.getKey(), size);
                writeValue(fileChannel, record.getValue(), size);
            }
        }
        if (mappedByteBuffer != null) {
            try {
                CLEAN.invoke(null, mappedByteBuffer);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException(e);
            }
        }

        Files.move(tmpFilePath, saveFilePath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    private void writeValue(FileChannel fileChannel, @Nullable ByteBuffer value, ByteBuffer size) throws IOException {
        if (value == null) {
            saveAndWriteSize(fileChannel, size, -1);
        } else {
            saveAndWriteSize(fileChannel, size, value.remaining());
            fileChannel.write(value);
        }
    }

    private void saveAndWriteSize(FileChannel fileChannel, ByteBuffer sizeBuffer, int size) throws IOException {
        sizeBuffer.position(0);
        sizeBuffer.putInt(size);
        sizeBuffer.position(0);
        fileChannel.write(sizeBuffer);
    }

    private SortedMap<ByteBuffer, Record> map(@Nullable final ByteBuffer fromKey, @Nullable final ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return storage;
        }
        if (fromKey == null) {
            return storage.headMap(toKey);
        }
        if (toKey == null) {
            return storage.tailMap(fromKey);
        }
        return storage.subMap(fromKey, toKey);
    }
}
