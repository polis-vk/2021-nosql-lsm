package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MyDAOImplementation implements DAO {

    private static final String SAVE_FILE_NAME = "save_dao.data";
    private final SortedMap<ByteBuffer, Record> storage;
    private final DAOConfig config;


    /**
     * Implementation of DAO that save data to the memory.
     */
    public MyDAOImplementation(DAOConfig config) throws IOException {
        this.config = config;
        this.storage = new ConcurrentSkipListMap<>();
        Path path = config.getDir().resolve(SAVE_FILE_NAME);

        if (Files.exists(path)) {
            try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
                ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
                ByteBuffer key;
                ByteBuffer value;
                while (fileChannel.position() != fileChannel.size()) {
                    key = readValue(fileChannel, size);
                    value = readValue(fileChannel, size);
                    Record record = Record.of(key, value);
                    storage.put(record.getKey(), record);
                }
            }
        }
    }

    private ByteBuffer readValue(FileChannel fileChannel, ByteBuffer temp) throws IOException {
        temp.position(0);
        while (temp.position() != temp.capacity()) {
            fileChannel.read(temp);
        }
        temp.position(0);
        ByteBuffer value = ByteBuffer.allocate(temp.getInt());
        while (value.position() != value.capacity()) {
            fileChannel.read(value);
        }
        return value.position(0);
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
        Files.deleteIfExists(config.getDir().resolve(SAVE_FILE_NAME));
        Path path = config.getDir().resolve(SAVE_FILE_NAME);

        try (FileChannel fileChannel =
                     FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
            ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            for (Record record : storage.values()) {
                writeValueAndKey(record, fileChannel, size);
            }
        }
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
