package ru.mail.polis.lsm;

import jdk.dynalink.StandardOperation;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

public class DaoImpl implements DAO{
    private final DAOConfig config;
    private final NavigableMap<ByteBuffer, Record> map;
    private static final String SAVE_FILE_NAME = "save.dat";

    public DaoImpl(DAOConfig config) throws IOException {
        this.config = config;
        this.map = new ConcurrentSkipListMap<>();
        Path resolve = config.getDir().resolve(SAVE_FILE_NAME);
        if (Files.exists(resolve)) {
            try(FileChannel fileChannel = FileChannel.open(resolve, StandardOpenOption.READ)) {
               ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
                while (fileChannel.position() < fileChannel.size()) {
                    ByteBuffer key = readBuffer(fileChannel, buffer);

                    ByteBuffer value = readBuffer(fileChannel, buffer);

                    map.put(key, new Record(key, value));
                }
            }
        }
    }

    private ByteBuffer readBuffer(FileChannel fileChannel, ByteBuffer buffer) throws IOException {
        buffer.position(0);
        fileChannel.read(buffer);
        buffer.position(0);
        ByteBuffer tmp = ByteBuffer.allocate(buffer.getInt());
        fileChannel.read(tmp);
        tmp.position(0);
        return tmp;
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return  map(fromKey, toKey).values().stream()
                .filter(record -> record.getValue() != null)
                .iterator();
    }

    private SortedMap<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey,@Nullable ByteBuffer toKey) {
        if ((fromKey == null) && (toKey == null))
                return map;

        if (fromKey == null)
            return map.headMap(toKey);

        if (toKey == null)
            return map.tailMap(fromKey);

        return map.subMap(fromKey, toKey);
    }

    @Override
    public void upsert(Record record) {
        map.put(record.getKey(), record);
    }

    @Override
    public void close() throws IOException {
        Files.deleteIfExists(config.getDir().resolve(SAVE_FILE_NAME));
        Path file = config.getDir().resolve(SAVE_FILE_NAME);
        try(FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
            ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            for (Record record : map.values()) {
                if (record.getValue() != null) {
                    writeInt(record.getKey(), fileChannel, size);
                    writeInt(record.getValue(), fileChannel, size);
                }
            }
        }
    }

    private static void writeInt(ByteBuffer value, WritableByteChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        tmp.putInt(value.remaining());
        tmp.position(0);
        channel.write(tmp);
        channel.write(value);
    }

}
