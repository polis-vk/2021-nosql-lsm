package ru.mail.polis.lsm.gromovmaxim;

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
    private static final String DATA_BASE = "database.dat";
    private final DAOConfig config;

    public NotJustInMemoryDAO(DAOConfig config) {
        this.config = config;

        final Path path = config.getDir().resolve(DATA_BASE);

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
            } catch (IOException e) {
                e.printStackTrace();
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
        Files.deleteIfExists(config.getDir().resolve(DATA_BASE));

        final Path file = config.getDir().resolve(DATA_BASE);

        try (FileChannel fileChannel =
                     FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
            final ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            for (final Record record : storage.values()) {
                writeInt(record.getKey(), fileChannel, size);
                writeInt(record.getValue(), fileChannel, size);
            }
        }
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

    private void writeInt(ByteBuffer value, WritableByteChannel channel, ByteBuffer byteBuffer) throws IOException {
        byteBuffer.position(0);
        byteBuffer.putInt(value.remaining());
        byteBuffer.position(0);
        channel.write(byteBuffer);
        channel.write(value);
    }

    private ByteBuffer readInt(ReadableByteChannel channel, ByteBuffer byteBuffer) throws IOException {
        byteBuffer.position(0);
        channel.read(byteBuffer);
        byteBuffer.position(0);
        final ByteBuffer returnBuff = ByteBuffer.allocate(byteBuffer.getInt());
        channel.read(returnBuff);
        returnBuff.position(0);
        return returnBuff;
    }
}
