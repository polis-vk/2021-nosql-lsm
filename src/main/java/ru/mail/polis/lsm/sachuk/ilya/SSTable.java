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
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class SSTable {

    private static final String SAVE_FILE_NAME = "save";
    private static final String TMP_FILE_NAME = "tmp";

    private Path savePath;
    private Path tmpPath;

    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();

    private static MappedByteBuffer mappedByteBuffer;


    SSTable(Path filePath) throws IOException {
        this.savePath = filePath;
        this.tmpPath = Paths.get(filePath.toString() + "tmp");

        restoreStorage();
    }

    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return map(fromKey, toKey).values().iterator();
    }

    private Map<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {

        if (fromKey == null && toKey == null) {
            return storage;
        } else if (fromKey == null) {
            return storage.headMap(toKey);
        } else if (toKey == null) {
            return storage.tailMap(fromKey);
        } else {
            return storage.subMap(fromKey, toKey);
        }
    }

    static SSTable save(Iterator<Record> iterators, Path dir) throws IOException {

        Path tmp = Path.of(dir.toString() + "tmp");

        try (FileChannel fileChannel = FileChannel.open(
                dir,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {

            while (iterators.hasNext()) {
                Record record = iterators.next();
                writeToFile(fileChannel, record.getKey());
                writeToFile(fileChannel, record.getValue());
            }
            fileChannel.force(false);
        }


//        Files.deleteIfExists(dir);
//        Files.move(tmp, dir, StandardCopyOption.ATOMIC_MOVE);

        return new SSTable(dir);
    }

    static List<SSTable> loadFromDir(Path dir) throws IOException {

        List<SSTable> listSSTables = new ArrayList<>();

        List<Path> paths;

        try (Stream<Path> streamPaths = Files.walk(Paths.get(dir.toUri()))) {
            paths = streamPaths.filter(Files::isRegularFile)
                    .collect(Collectors.toList());
        }

        for (Path path : paths) {
            listSSTables.add(new SSTable(path));
        }

        return listSSTables;
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

//    private static void saveF() throws IOException {
//
//        try (FileChannel fileChannel = FileChannel.open(
//                tmpPath,
//                StandardOpenOption.CREATE_NEW,
//                StandardOpenOption.WRITE,
//                StandardOpenOption.TRUNCATE_EXISTING
//        )) {
//
//            for (final Map.Entry<ByteBuffer, Record> byteBufferRecordEntry : storage.entrySet()) {
//                writeToFile(fileChannel, byteBufferRecordEntry.getKey());
//                writeToFile(fileChannel, byteBufferRecordEntry.getValue().getValue());
//            }
//            fileChannel.force(false);
//        }
//    }

    private static void writeToFile(FileChannel fileChannel, ByteBuffer value) throws IOException {

        ByteBuffer byteBuffer = ByteBuffer.allocate(value.capacity());
        ByteBuffer secBuf = ByteBuffer.allocate(Integer.BYTES);

        secBuf.putInt(value.remaining());
        byteBuffer.put(value);

        write(fileChannel, secBuf);
        write(fileChannel, byteBuffer);
    }

    private static void write(FileChannel fileChannel, ByteBuffer byteBuffer) throws IOException {
        byteBuffer.flip();
        fileChannel.write(byteBuffer);
        byteBuffer.compact();
    }

    void close() throws IOException {
        if (mappedByteBuffer != null) {
            clean();
        }
    }

    private static void clean() throws IOException {
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
