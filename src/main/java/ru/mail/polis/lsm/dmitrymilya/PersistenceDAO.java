package ru.mail.polis.lsm.dmitrymilya;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nonnull;
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

/**
 * Persistent DAO implementation.
 */
public class PersistenceDAO implements DAO {

    private static final Method CLEAN;

    static {
        try {
            Class<?> clazz = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = clazz.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    private static final String SAVE_FILE_NAME = "save.dat";
    private static final String TMP_FILE_NAME = "tmp.dat";
    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();

    private final Path saveFileName;
    private final Path tmpFileName;

    private final MappedByteBuffer mappedByteBuffer;

    /**
     * Constructs a Persistent DAO object, getting data from the save file.
     *
     * @param config DAO config
     */
    public PersistenceDAO(DAOConfig config) throws IOException {

        saveFileName = config.getDir().resolve(SAVE_FILE_NAME);
        tmpFileName = config.getDir().resolve(TMP_FILE_NAME);
        if (Files.exists(saveFileName)) {
            try (FileChannel fileChannel = FileChannel.open(saveFileName, StandardOpenOption.READ)) {
                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
                while (mappedByteBuffer.hasRemaining()) {
                    int keySize = mappedByteBuffer.getInt();
                    ByteBuffer key = mappedByteBuffer.slice().limit(keySize).asReadOnlyBuffer();

                    mappedByteBuffer.position(mappedByteBuffer.position() + keySize);

                    int valueSize = mappedByteBuffer.getInt();
                    ByteBuffer value = mappedByteBuffer.slice().limit(valueSize).asReadOnlyBuffer();

                    mappedByteBuffer.position(mappedByteBuffer.position() + valueSize);

                    storage.put(key, Record.of(key, value));
                }
            }
        } else {
            if (Files.exists(tmpFileName)) {
                Files.move(tmpFileName, saveFileName, StandardCopyOption.ATOMIC_MOVE);
            }
            mappedByteBuffer = null;
        }
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return map(fromKey, toKey).values().iterator();
    }

    private SortedMap<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
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

    @Override
    public void upsert(@Nonnull Record record) {
        if (record.getValue() == null) {
            storage.remove(record.getKey());
        } else {
            storage.put(record.getKey(), record);
        }
    }

    @Override
    public void close() throws IOException {
        try (FileChannel fileChannel = FileChannel.open(
                tmpFileName,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {
            final ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            for (final Record record : storage.values()) {
                writeInt(record.getKey(), fileChannel, size);
                writeInt(record.getValue(), fileChannel, size);
            }
            fileChannel.force(false);
        }

        if (mappedByteBuffer != null) {
            try {
                CLEAN.invoke(null, mappedByteBuffer);
            } catch (InvocationTargetException | IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        }

        Files.deleteIfExists(saveFileName);
        Files.move(tmpFileName, saveFileName, StandardCopyOption.ATOMIC_MOVE);
    }

    private void writeInt(ByteBuffer value, WritableByteChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        tmp.putInt(value.remaining());
        tmp.position(0);
        channel.write(tmp);
        channel.write(value);
    }

}
