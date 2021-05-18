package ru.mail.polis.lsm.saveliyschur;

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
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MyDAO implements DAO {

    private volatile ConcurrentSkipListMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    private final DAOConfig config;
    private static final String FILE_SAVE = "save.dat";

    public MyDAO(DAOConfig config) throws IOException {
        this.config = config;

        Path file = config.getDir().resolve(FILE_SAVE);

        if(Files.exists(file)){
            final ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ)) {
                while (fileChannel.position() < fileChannel.size()) {
                    ByteBuffer key = readValue(fileChannel, size);
                    storage.put(key, Record.of(key, readValue(fileChannel, size)));
                }
            }
        }
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        ConcurrentNavigableMap<ByteBuffer, Record> storageHash = storage.clone();
        return getSubMap(fromKey, toKey, storageHash).values().stream().filter(x -> x.getValue() != null).iterator();
    }

    @Override
    public void upsert(Record record) {
        storage.put(record.getKey(), record);
    }

    @Override
    public void close() throws IOException {
        Files.deleteIfExists(config.getDir().resolve(FILE_SAVE));

        Path file = config.getDir().resolve(FILE_SAVE);

        try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            for (Record record : storage.values()) {
                if (record.getValue() == null) {
                    continue;
                }
                writeInt(record.getKey(), fileChannel, size);
                writeInt(record.getValue(), fileChannel, size);
            }
        }
    }

    public DAOConfig getConfig() {
        return config;
    }

    private ConcurrentNavigableMap<ByteBuffer, Record> getSubMap(@Nullable ByteBuffer fromKey,
                                                                 @Nullable ByteBuffer toKey,
                                                                 ConcurrentNavigableMap<ByteBuffer, Record> map) {
        if (fromKey == null && toKey == null)
            return map;
        else if (fromKey == null)
            return map.headMap(toKey);
        else if (toKey == null)
            return map.tailMap(fromKey);
        else
            return map.subMap(fromKey, toKey);
    }

    private static ByteBuffer readValue(ReadableByteChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        channel.read(tmp);
        tmp.position(0);
        ByteBuffer byteBuffer = ByteBuffer.allocate(tmp.getInt());
        channel.read(byteBuffer);
        byteBuffer.position(0);
        return byteBuffer;
    }

    private static void writeInt(ByteBuffer value, WritableByteChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        tmp.putInt(value.remaining());
        tmp.position(0);
        channel.write(tmp);
        channel.write(value);
    }
}
