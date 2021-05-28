package ru.mail.polis.lsm.igorsamokhin;

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
    private static final String FILE_NAME = "save.dat";

    private final Path filePath;

    /**
     * Create DAO object.
     *
     * @param config - objects contains directory with data files
     */
    public NotJustInMemoryDAO(DAOConfig config) throws IOException {
        this.config = config;
        filePath = config.getDir().resolve(FILE_NAME);

        if (!Files.exists(filePath)) {
            return;
        }

        try (FileChannel fileChannel = FileChannel.open(filePath,
                StandardOpenOption.READ, StandardOpenOption.CREATE_NEW)) {
            final long size = fileChannel.size();
            final ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);

            while (fileChannel.position() != size) {
                ByteBuffer key = readValue(fileChannel, buffer);
                ByteBuffer value = readValue(fileChannel, buffer);
                storage.put(key, Record.of(key, value));
            }
        }
    }

    private ByteBuffer readValue(ReadableByteChannel channel, ByteBuffer tmp) throws IOException {
        int amount = 0;
        tmp.position(amount);
        while (amount <= tmp.remaining()) {
            amount += channel.read(tmp);
        }
        tmp.position(0);

        final int size = tmp.getInt();
        final ByteBuffer returnBuff = ByteBuffer.allocate(size);
        channel.read(returnBuff);
        returnBuff.position(0);
        return returnBuff;
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
        Files.deleteIfExists(filePath);
        Files.createDirectories(config.getDir());

        try (FileChannel fileChannel = FileChannel.open(filePath, StandardOpenOption.WRITE,
                StandardOpenOption.CREATE_NEW)) {
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
}
