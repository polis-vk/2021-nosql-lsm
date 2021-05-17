package ru.mail.polis.lsm.dmitry_milya;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nonnull;
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

public class PersistenceDAO implements DAO {

    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();

    private final DAOConfig config;
    private static final String SAVE_FILE_NAME = "save.dat";

    public PersistenceDAO(DAOConfig config) {
        this.config = config;

        final Path resolve = config.getDir().resolve(SAVE_FILE_NAME);
        if (Files.exists(resolve)) {
            try (FileChannel fileChannel = FileChannel.open(resolve, StandardOpenOption.READ,
                    StandardOpenOption.CREATE_NEW)) {
                final ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
                while (fileChannel.position() < fileChannel.size()) {
                    final ByteBuffer key = readValue(fileChannel, size);
                    final ByteBuffer value = readValue(fileChannel, size);
                    storage.put(key, Record.of(key, value));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private ByteBuffer readValue(ReadableByteChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        channel.read(tmp);
        tmp.position(0);
        final ByteBuffer byteBuffer = ByteBuffer.allocate(tmp.getInt());
        channel.read(byteBuffer);
        byteBuffer.position(0);
        return byteBuffer;
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return map(fromKey, toKey).values().iterator();
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

    @Override
    public void upsert(@Nonnull Record record) {
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

        if (!file.toFile().exists()) {
            Files.createFile(file);
        }

        try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.WRITE)) {
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
