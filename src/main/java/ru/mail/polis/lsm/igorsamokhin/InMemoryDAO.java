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
import java.util.Arrays;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDAO implements DAO {
    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    private final DAOConfig config;
    private static final String FILE_NAME = "save.dat";

    public InMemoryDAO(DAOConfig config) {
        this.config = config;

        Path path = config.getDir().resolve(FILE_NAME);
        if (!path.toFile().exists()) {
            return;
        }

        try (FileChannel fileChannel = FileChannel.open(path,
                StandardOpenOption.READ, StandardOpenOption.CREATE_NEW)) {
            long size = fileChannel.size();
            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
            while (fileChannel.position() != size) {
                ByteBuffer key = readValue(fileChannel, buffer);
                ByteBuffer value = readValue(fileChannel, buffer);
                storage.put(key, Record.of(key, value));
            }
        } catch (IOException e) {
            System.err.println(Arrays.toString(e.getStackTrace()));
        }
    }

    private ByteBuffer readValue(ReadableByteChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        channel.read(tmp);
        tmp.position(0);
        int size = tmp.getInt();
        ByteBuffer returnBuff = ByteBuffer.allocate(size);
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
            final Record r = storage.get(record.getKey());
            if (r != null) {
                storage.remove(r.getKey());
            }
        } else {
            storage.put(record.getKey(), record);
        }
    }

    @Override
    public void close() throws IOException {
        Files.deleteIfExists(config.getDir().resolve(FILE_NAME));

        Path dir = config.getDir();
        Path file = config.getDir().resolve(FILE_NAME);
        if (!dir.toFile().exists()) {
            Files.createDirectory(dir);
        }
        if (!file.toFile().exists()) {
            Files.createFile(file);
        }

        try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.WRITE)) {
            ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            for (Record record : storage.values()) {
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
