package ru.mail.polis.lsm.sachuk.ilya;

import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

class SSTable {

    private static final String SAVE_FILE_NAME = "save";
    private static final String TMP_FILE_NAME = "tmp";

    private Path savePath;
    private Path tmpPath;

    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();

    private MappedByteBuffer mappedByteBuffer;


    static List<SSTable> loadFromDir(Path dir) {
        return Collections.emptyList();
    }

    SSTable(Path filePath) {

    }

    static SSTable save(Iterator<Record> iterators, Path dir) {
        return null;
    }

    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return Collections.emptyIterator();
    }

    private void restoreStorage() throws IOException {
        if (Files.exists(savePath)) {
            try (FileChannel fileChannel = FileChannel.open(savePath, StandardOpenOption.READ)) {

                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());

                while (mappedByteBuffer.hasRemaining()) {
                    ByteBuffer keyByteBuffer = readFromFile(mappedByteBuffer);
                    ByteBuffer valueByteBuffer = readFromFile(mappedByteBuffer);

                    storage.put(keyByteBuffer, Record.of(keyByteBuffer, valueByteBuffer));
                }
            }
        }
    }

    private ByteBuffer readFromFile(MappedByteBuffer mappedByteBuffer) throws IOException {
        int length = mappedByteBuffer.getInt();

        ByteBuffer byteBuffer = mappedByteBuffer.slice().limit(length).asReadOnlyBuffer();
        mappedByteBuffer.position(mappedByteBuffer.position() + length);

        return byteBuffer;
    }

    private void save() throws IOException {

        try (FileChannel fileChannel = FileChannel.open(
                tmpPath,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {

            for (final Map.Entry<ByteBuffer, Record> byteBufferRecordEntry : storage.entrySet()) {
                writeToFile(fileChannel, byteBufferRecordEntry.getKey());
                writeToFile(fileChannel, byteBufferRecordEntry.getValue().getValue());
            }
            fileChannel.force(false);
        }
    }

    private void writeToFile(FileChannel fileChannel, ByteBuffer value) throws IOException {

        ByteBuffer byteBuffer = ByteBuffer.allocate(value.capacity());
        ByteBuffer secBuf = ByteBuffer.allocate(Integer.BYTES);

        secBuf.putInt(value.remaining());
        byteBuffer.put(value);

        write(fileChannel, secBuf);
        write(fileChannel, byteBuffer);
    }

    private void write(FileChannel fileChannel, ByteBuffer byteBuffer) throws IOException {
        byteBuffer.flip();
        fileChannel.write(byteBuffer);
        byteBuffer.compact();
    }

    private void clean() throws IOException {
        try {
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            Object unsafe = unsafeField.get(null);
            Method invokeCleaner = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
            invokeCleaner.invoke(unsafe, mappedByteBuffer);
        } catch (ClassNotFoundException | NoSuchFieldException | NoSuchMethodException
                | IllegalAccessException | InvocationTargetException e) {
            throw new IOException(e);
        }
    }
}
