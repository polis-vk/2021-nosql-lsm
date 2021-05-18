package ru.mail.polis.lsm.shabinsky;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;

import java.io.IOException;
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
    private static final String SAVE_FILE_NAME = "save1.dat";

    /**
     * NotJustInMemoryDAO constructor
     *
     * @param config {@link DAOConfig}
     * @throws IOException raises an exception
     */
    public NotJustInMemoryDAO(DAOConfig config) throws IOException {
        this.config = config;

        final Path path = config.getDir().resolve(SAVE_FILE_NAME);

        if (Files.exists(path)) {
            try (FileChannel fileChannel =
                     FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.CREATE_NEW)) {
                final ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
                ByteBuffer key;
                ByteBuffer value;
                while (fileChannel.position() < fileChannel.size()) {
                    key = readInt(fileChannel, buffer);
                    value = readInt(fileChannel, buffer);
                    storage.put(key, Record.of(key, value));
                }
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
        Files.deleteIfExists(config.getDir().resolve(SAVE_FILE_NAME));

        final Path file = config.getDir().resolve(SAVE_FILE_NAME);

        try (FileChannel fileChannel =
                 FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
            final ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            for (final Record record : storage.values()) {
                writeInt(record.getKey(), fileChannel, size);
                writeInt(record.getValue(), fileChannel, size);
            }
        }
    }

    private void writeInt(ByteBuffer value, WritableByteChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        tmp.putInt(value.remaining());
        tmp.position(0);
        channel.write(tmp);
        channel.write(value);
    }

    private ByteBuffer readInt(ReadableByteChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        channel.read(tmp);
        tmp.position(0);
        final ByteBuffer returnBuff = ByteBuffer.allocate(tmp.getInt());
        channel.read(returnBuff);
        returnBuff.position(0);
        return returnBuff;
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
}
