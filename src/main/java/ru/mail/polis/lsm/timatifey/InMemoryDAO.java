package ru.mail.polis.lsm.timatifey;

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
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDAO implements DAO {

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

    private final Path directory;
    private final NavigableMap<ByteBuffer, Record> storage;
    private final MappedByteBuffer mappedByteBuffer;

    private static final String FILE_NAME = "IN_MEMORY_DAO_STORAGE_SAVE.dat";
    private static final String TMP_FILE_NAME = "IN_MEMORY_DAO_STORAGE_TMP.dat";
    private final Path saveFileName;
    private final Path tmpFileName;


    /**
     * @param config - config of DAO for get directory
     */
    public InMemoryDAO(DAOConfig config) throws IOException {
        this.directory = config.getDir();
        this.storage = new ConcurrentSkipListMap<>();

        this.saveFileName = directory.resolve(FILE_NAME);
        this.tmpFileName = directory.resolve(TMP_FILE_NAME);

        if (!Files.exists(saveFileName)) {
            if (Files.exists(tmpFileName)) {
                Files.move(tmpFileName, saveFileName, StandardCopyOption.ATOMIC_MOVE);
            } else {
                mappedByteBuffer = null;
                return;
            }
        }
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
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return getSortedMap(fromKey, toKey).values().iterator();
    }

    private SortedMap<ByteBuffer, Record> getSortedMap(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if ((fromKey == null) && (toKey == null)) {
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
    public void upsert(Record record) {
        if (record.getValue() == null) {
            storage.remove(record.getKey());
            return;
        }
        storage.put(record.getKey(), record);
    }

    @Override
    public void close() throws IOException {
        try (FileChannel fileChannel = FileChannel.open(
                tmpFileName,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {
            ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            for (Record record : storage.values()) {
                writeInteger(record.getKey(), fileChannel, size);
                writeInteger(record.getValue(), fileChannel, size);
            }
            fileChannel.force(false);
        }

        if (mappedByteBuffer != null) {
            try {
                CLEAN.invoke(null, mappedByteBuffer);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IOException(e);
            }
        }

        Files.deleteIfExists(saveFileName);
        Files.move(tmpFileName, saveFileName, StandardCopyOption.ATOMIC_MOVE);
    }

    private void writeInteger(ByteBuffer key, FileChannel channel, ByteBuffer size) throws IOException {
        size.position(0);
        size.putInt(key.remaining());
        size.position(0);
        channel.write(size);
        channel.write(key);
    }

}
