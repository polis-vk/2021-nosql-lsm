package ru.mail.polis.lsm.igorsamokhin;

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
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

class SSTable {
    private static final Method CLEAN;
    private static final String TMP_FILE_SUFFIX = "_temp";

    private final SortedMap<ByteBuffer, Record> memoryStorage = new ConcurrentSkipListMap<>();
    private final MappedByteBuffer mmap;

    static {
        try {
            Class<?> name = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = name.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    public SSTable(Path file) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ)) {
            mmap = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());

            while (mmap.hasRemaining()) {
                ByteBuffer key = readValue(mmap);
                ByteBuffer value = readValue(mmap);

                memoryStorage.put(key, Record.of(key, value));
            }
        }
    }

    public void close() throws IOException {
        if (mmap != null) {
            try {
                CLEAN.invoke(null, mmap);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IOException(e);
            }
        }
    }

    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        synchronized (this) {
            return getSubMap(fromKey, toKey).values().iterator();
        }
    }

    private SortedMap<ByteBuffer, Record> getSubMap(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return memoryStorage;
        } else if (fromKey == null) {
            return memoryStorage.headMap(toKey);
        } else if (toKey == null) {
            return memoryStorage.tailMap(fromKey);
        }
        return memoryStorage.subMap(fromKey, toKey);
    }

    private ByteBuffer readValue(MappedByteBuffer map) {
        int size = map.getInt();
        if (size == 0) {
            return null;
        }
        ByteBuffer value = map.slice().limit(size).asReadOnlyBuffer();
        map.position(map.position() + size);
        return value;
    }

    static List<SSTable> loadFromDir(Path dir) throws IOException {
        File[] files = dir.toFile().listFiles();
        ArrayList<SSTable> ssTables = new ArrayList<>();
        if (files == null) {
            return ssTables;
        }

        for (File file : files) {
            if (!file.getName().endsWith(TMP_FILE_SUFFIX)) {
                ssTables.add(new SSTable(file.toPath()));
            }
        }
        return ssTables;
    }

    static SSTable write(Iterator<Record> records, Path file) throws IOException {
        String first = file.toString() + TMP_FILE_SUFFIX;
        Path tmpFilePath = Path.of(first);
        Files.deleteIfExists(tmpFilePath);

        try (FileChannel fileChannel = FileChannel.open(
                tmpFilePath,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE_NEW
        )) {
            final ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            while (records.hasNext()) {
                Record record = records.next();
                writeInt(record.getKey(), fileChannel, size);
                writeInt(record.getValue(), fileChannel, size);
            }
            fileChannel.force(false);
        }

        Files.deleteIfExists(file);
        Files.move(tmpFilePath, file, StandardCopyOption.ATOMIC_MOVE);
        Files.deleteIfExists(tmpFilePath);
        return new SSTable(file);
    }

    private static void writeInt(@Nullable ByteBuffer value, WritableByteChannel channel, ByteBuffer tmp) throws IOException {
        if (value == null) {
            tmp.putInt(0);
            channel.write(tmp);
            return;
        }

        tmp.position(0);
        tmp.putInt(value.remaining());
        tmp.position(0);
        channel.write(tmp);
        channel.write(value);
    }
}
