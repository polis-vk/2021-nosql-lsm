package ru.mail.polis.lsm.serikuly_miras;

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
    private static final String FILE_NAME = "save.dat";
    private final DAOConfig config;

    public NotJustInMemoryDAO(DAOConfig config) throws IOException {
        this.config = config;
        Path path = config.getDir().resolve(FILE_NAME);
        if(Files.exists(path)){
            try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.CREATE_NEW)) {
                ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
                while (fileChannel.position() != fileChannel.size()) {
                    Record record = readRecord(fileChannel, buffer);
                    storage.put(record.getKey(), record);
                }
            }
        }

    }

    private Record readRecord(ReadableByteChannel channel, ByteBuffer tmp) throws IOException {
        return Record.of(readValue(channel, tmp), readValue(channel, tmp));
    }

    private ByteBuffer readValue(ReadableByteChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        channel.read(tmp);
        tmp.position(0);
        final ByteBuffer buffer = ByteBuffer.allocate(tmp.getInt());
        channel.read(buffer);
        buffer.position(0);
        return buffer;
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return subMap(fromKey, toKey).values().iterator();
    }

    private SortedMap<ByteBuffer, Record> subMap(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
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
        Files.deleteIfExists(config.getDir().resolve(FILE_NAME));
        Path path = config.getDir().resolve(FILE_NAME);
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
            for (Record record : storage.values()) {
                writeValue(record.getKey(), fileChannel, buffer);
                writeValue(record.getValue(), fileChannel, buffer);
            }
        }
    }

    private void writeValue(ByteBuffer value, WritableByteChannel channel, ByteBuffer buffer) throws IOException {
        buffer.position(0);
        buffer.putInt(value.remaining());
        buffer.position(0);
        channel.write(buffer);
        channel.write(value);
    }
}
