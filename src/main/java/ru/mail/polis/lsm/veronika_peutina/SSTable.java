package ru.mail.polis.lsm.veronika_peutina;

import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

class SSTable {
    private static final Method CLEAN;

    static {
        try {
            Class<?> aClass = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = aClass.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }


    private final MappedByteBuffer map;

    SSTable(Path file) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            map = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        }
    }

    static List<SSTable> loadFromDir(Path dir, int size) throws IOException {
        List<Path> files = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            files.add(dir.resolve("SSTable" + i + ".dat"));
        }

        if (files.size() == 0) {
            return new ArrayList<>();
        }

        List<SSTable> listSSTables = new ArrayList<>();

        for (Path it : files) {
            listSSTables.add(new SSTable(it));
        }

        return listSSTables;
    }

    public Iterator<Record> range(SortedMap<ByteBuffer, Record> tableStorage, @Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return getSubMap(tableStorage, fromKey, toKey).values().iterator();
    }

    static SortedMap<ByteBuffer, Record> getSubMap(SortedMap<ByteBuffer, Record> tableStorage, @Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return tableStorage;
        }
        if (fromKey == null) {
            return tableStorage.headMap(toKey);
        }
        if (toKey == null) {
            return tableStorage.tailMap(fromKey);
        }
        return tableStorage.subMap(fromKey, toKey);
    }

    public void close() throws IOException {
        if (map == null) {
            return;
        }

        try {
            CLEAN.invoke(null, map);

        } catch (InvocationTargetException | IllegalAccessException e) {
            throw new IOException();
        }
    }

    static SSTable write(Iterator<Record> recordIterator, Path file) throws IOException {
        Files.deleteIfExists(file);

        try (FileChannel fileChannel = FileChannel.open(
                file,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {
            ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            for (Iterator<Record> it = recordIterator; it.hasNext(); ) {
                Record record = it.next();
                writeRecord(record, fileChannel, size);
            }
            fileChannel.force(false);
        }
        return new SSTable(file);
    }


    private static void writeRecord(Record record, WritableByteChannel channel, ByteBuffer tmp) throws IOException {
        write(record.getKey(), channel, tmp);
        write(record.getValue(), channel, tmp);
    }

    private static void write(ByteBuffer val, WritableByteChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        tmp.putInt(val.remaining());
        tmp.position(0);
        channel.write(tmp);
        channel.write(val);
    }


}
