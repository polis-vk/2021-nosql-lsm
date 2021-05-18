package ru.mail.polis.lsm.alyonazakharova;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class NotJustInMemoryDAO implements DAO {

    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    private final DAOConfig config;
    private static final String SAVE_FILE_NAME = "save.dat";

    public NotJustInMemoryDAO(DAOConfig config) throws IOException {
        this.config = config;

        Path path = config.getDir().resolve(SAVE_FILE_NAME);

        if (Files.exists(path)) {
            try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
                ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
                while (fileChannel.read(size) > 0) {
                    ByteBuffer key = readValue(fileChannel, size);
                    fileChannel.read(size.flip());
                    ByteBuffer value = readValue(fileChannel, size);
                    storage.put(key, Record.of(key, value));
                }
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
        Path file = config.getDir().resolve(SAVE_FILE_NAME);

        try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.WRITE,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

            ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);

            for (Record record : storage.values()) {
                writeValue(record.getKey(), fileChannel, size);
                writeValue(record.getValue(), fileChannel, size);
            }
        }
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

    private static ByteBuffer readValue(ReadableByteChannel channel, ByteBuffer size) throws IOException {
        size.flip();
        ByteBuffer value = ByteBuffer.allocate(size.getInt());
        channel.read(value);
        return value.flip();
    }
}
