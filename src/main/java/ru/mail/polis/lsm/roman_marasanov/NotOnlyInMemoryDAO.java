package ru.mail.polis.lsm.roman_marasanov;

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
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class NotOnlyInMemoryDAO implements DAO {

    private static final String DATA_FILE_NAME = "data.dat";
    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    private final Path filePath;


    /**
     * Create DAO in memory using data, that saved in file
     * @param config contains path
     */
    public NotOnlyInMemoryDAO(DAOConfig config) {
        this.filePath = config.getDir().resolve(DATA_FILE_NAME);

        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            long channelSize = channel.size();
            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
            ByteBuffer key;
            ByteBuffer value;
            while (channel.position() != channelSize) {
                key = readBuffer(channel, buffer);
                value = readBuffer(channel, buffer);
                storage.put(key, Record.of(key, value));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private ByteBuffer readBuffer(FileChannel channel, ByteBuffer buffer) throws IOException {
        buffer.position(0);
        channel.read(buffer);
        buffer.position(0);
        ByteBuffer byteBuffer = ByteBuffer.allocate(buffer.getInt());
        channel.read(byteBuffer);
        byteBuffer.position(0);
        return byteBuffer;
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return storageInRange(fromKey, toKey).values().iterator();
    }

    private SortedMap<ByteBuffer, Record> storageInRange(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
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
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);

            for (Record record: storage.values()) {
                writeInt(record.getKey(), channel, buffer);
                writeInt(record.getValue(), channel, buffer);
            }
        }
    }

    private void writeInt(ByteBuffer value, FileChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        tmp.putInt(value.remaining());
        tmp.position(0);
        channel.write(tmp);
        channel.write(value);
    }
}
