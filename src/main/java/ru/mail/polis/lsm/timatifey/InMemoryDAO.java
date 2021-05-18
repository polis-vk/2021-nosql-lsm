package ru.mail.polis.lsm.timatifey;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDAO implements DAO {

    private final Path directory;
    private final NavigableMap<ByteBuffer, Record> storage;
    private static final String FILE_NAME = "IN_MEMORY_DAO_STORAGE";

    public InMemoryDAO(DAOConfig config) {
        this.directory = config.getDir();
        this.storage = new ConcurrentSkipListMap<>();
        try {
            initStorageByInMemoryFile();
        } catch (IOException ignore) { }
    }

    private void initStorageByInMemoryFile() throws IOException {
        Path file = directory.resolve(FILE_NAME);
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            long channelSize = channel.size();
            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
            ByteBuffer key;
            ByteBuffer value;
            while (channel.position() != channelSize) {
                key = readByteBuffer(channel, buffer);
                value = readByteBuffer(channel, buffer);
                storage.put(key, Record.of(key, value));

                String sb = "READ: " + key.toString() +
                        "\t" +
                        value.toString() +
                        "\n";
                System.out.println(sb);
            }
        }
    }

    private ByteBuffer readByteBuffer(FileChannel channel, ByteBuffer buffer) throws IOException {
        buffer.position(0);
        channel.read(buffer);
        buffer.position(0);

        ByteBuffer readByte = ByteBuffer.allocate(buffer.getInt());
        channel.read(readByte);
        readByte.position(0);
        return readByte;
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
        Files.deleteIfExists(directory.resolve(FILE_NAME));
        Path file = directory.resolve(FILE_NAME);
        try(FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);

            for (Record record: storage.values()) {
                writeInteger(record.getKey(), channel, buffer);
                writeInteger(record.getValue(), channel, buffer);

                String sb = "WRITE: " + record.getKey().toString() +
                        "\t" +
                        record.getValue().toString() +
                        "\n";
                System.out.println(sb);
            }
        }
    }

    private void writeInteger(ByteBuffer key, FileChannel channel, ByteBuffer size) throws IOException {
        size.position(0);
        size.putInt(key.remaining());
        size.position(0);
        channel.write(size);
        channel.write(key);
    }

}
