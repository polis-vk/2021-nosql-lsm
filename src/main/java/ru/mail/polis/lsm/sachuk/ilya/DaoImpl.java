package ru.mail.polis.lsm.sachuk.ilya;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DaoImpl implements DAO {

    private static final String SAVE_FILE_NAME = "save";
    private static final String TMP_FILE_NAME = "tmp";

    private final Path savePath;
    private final Path tmpPath;
    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();

    private MappedByteBuffer mappedByteBuffer;

    /**
     * Constructor that initialize path and restore storage.
     *
     * @param dirPath path to the directory in which will be created file
     * @throws IOException is thrown when an I/O error occurs.
     */
    public DaoImpl(Path dirPath) throws IOException {
        this.savePath = dirPath.resolve(Paths.get(SAVE_FILE_NAME));
        this.tmpPath = dirPath.resolve(Paths.get(TMP_FILE_NAME));

        if (!Files.exists(savePath)) {
            if (Files.exists(tmpPath)) {
                Files.move(tmpPath, savePath, StandardCopyOption.ATOMIC_MOVE);
            } else {
                mappedByteBuffer = null;
                return;
            }

        }
        restoreStorage();
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
        save();

        if (mappedByteBuffer != null) {
            clean();
        }

        Files.deleteIfExists(savePath);
        Files.move(tmpPath, savePath, StandardCopyOption.ATOMIC_MOVE);
    }

    private Map<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {

        if (fromKey == null && toKey == null) {
            return storage;
        } else if (fromKey == null) {
            return storage.headMap(toKey);
        } else if (toKey == null) {
            return storage.tailMap(fromKey);
        } else {
            return storage.subMap(fromKey, toKey);
        }
    }

    private void save() throws IOException {

        try (FileChannel fileChannel = FileChannel.open(tmpPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {

            for (final Map.Entry<ByteBuffer, Record> byteBufferRecordEntry : storage.entrySet()) {
                writeToFile(fileChannel, byteBufferRecordEntry.getKey());
                writeToFile(fileChannel, byteBufferRecordEntry.getValue().getValue());
            }
            fileChannel.force(false);
        }
    }

    private void restoreStorage() throws IOException {
        if (Files.exists(savePath)) {
            try (FileChannel fileChannel = FileChannel.open(savePath, StandardOpenOption.READ)) {

                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());

                while (mappedByteBuffer.hasRemaining()) {
                    ByteBuffer keyByteBuffer = readFromFile(mappedByteBuffer);
                    ByteBuffer valueByteBuffer = readFromFile(mappedByteBuffer);

                    storage.put(keyByteBuffer, Record.of(keyByteBuffer, valueByteBuffer));
                }
            }
        }
    }

    private ByteBuffer readFromFile(MappedByteBuffer mappedByteBuffer) throws IOException {
        int length = mappedByteBuffer.getInt();

        ByteBuffer byteBuffer = mappedByteBuffer.slice().limit(length).asReadOnlyBuffer();
        mappedByteBuffer.position(mappedByteBuffer.position() + length);

        return byteBuffer;
    }

    private void writeToFile(FileChannel fileChannel, ByteBuffer value) throws
            IOException {

        ByteBuffer byteBuffer = ByteBuffer.allocate(value.capacity());
        ByteBuffer secBuf = ByteBuffer.allocate(Integer.BYTES);

        secBuf.putInt(value.remaining());
        byteBuffer.put(value);

        write(fileChannel, secBuf);
        write(fileChannel, byteBuffer);
    }

    private void write(FileChannel fileChannel, ByteBuffer byteBuffer) throws IOException {
        byteBuffer.flip();
        fileChannel.write(byteBuffer);
        byteBuffer.compact();
    }

    private void clean() {
        try {
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            Object unsafe = unsafeField.get(null);
            Method invokeCleaner = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
            invokeCleaner.invoke(unsafe, mappedByteBuffer);
        } catch (ClassNotFoundException | NoSuchFieldException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }
}