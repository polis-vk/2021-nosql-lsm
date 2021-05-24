package ru.mail.polis.lsm;

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

public class MyDAOImplementation implements DAO {

    private static final Method CLEAN;

    static {
        try {
            Class<?> fileChannelImplClass = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = fileChannelImplClass.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    private final DAOConfig config;

    private final Path saveFileName;
    private final Path tempFileName;

    private final MappedByteBuffer mmap;

    /**
     * Implementation of DAO that save data to the memory.
     */
    public MyDAOImplementation(DAOConfig config) throws IOException {
        this.config = config;

        Path dir = config.getDir();
        saveFileName = dir.resolve("save.data");
        tempFileName = dir.resolve("temp.data");

        if (!Files.exists(saveFileName)) {
            if (Files.exists(tempFileName)) {
                Files.move(tempFileName, saveFileName, StandardCopyOption.ATOMIC_MOVE);
            } else {
                mmap = null;
                return;
            }
        }

        try (FileChannel fileChannel = FileChannel.open(saveFileName, StandardOpenOption.READ)) {
            mmap = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
            while (mmap.hasRemaining()) {
                int keySize = mmap.getInt();
                ByteBuffer key = mmap.slice().limit(keySize).slice();

                mmap.position(mmap.position() + keySize);

                int valueSize = mmap.getInt();
                ByteBuffer value = mmap.slice().limit(valueSize).slice();

                mmap.position(mmap.position() + valueSize);

                storage.put(key, Record.of(key, value));
            }
        }
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return map(fromKey, toKey).values().iterator();
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

        try (FileChannel fileChannel = FileChannel.open(
                tempFileName,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {
            ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            for (Record record : storage.values()) {
                writeValueAndKey(record, fileChannel, size);
            }
            fileChannel.force(false);
        }

        if (mmap != null) {
            try {
                CLEAN.invoke(null, mmap);
            } catch (IllegalAccessError | InvocationTargetException | IllegalAccessException e) {
                throw new IOException(e);
            }
        }

        Files.deleteIfExists(saveFileName);
        Files.move(tempFileName, saveFileName, StandardCopyOption.ATOMIC_MOVE);
    }

    private void writeValueAndKey(Record record, FileChannel fileChannel, ByteBuffer size) throws IOException {
        writeValue(record.getKey(), fileChannel, size);
        writeValue(record.getValue(), fileChannel, size);
    }

    private void writeValue(ByteBuffer value, WritableByteChannel fileChannel, ByteBuffer temp) throws IOException {
        temp.position(0);
        temp.putInt(value.remaining());
        temp.position(0);
        fileChannel.write(temp);
        fileChannel.write(value);
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

    public DAOConfig getConfig() {
        return config;
    }
}
