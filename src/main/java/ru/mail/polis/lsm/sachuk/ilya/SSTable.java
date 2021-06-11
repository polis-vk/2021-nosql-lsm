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
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
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

    private Path savePath;


    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();

    private MappedByteBuffer mappedByteBuffer;


    SSTable(Path filePath) throws IOException {
        this.savePath = filePath;

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

    static SSTable save(Iterator<Record> iterators, Path dir) throws IOException {

        Path tmp = Path.of(dir.toString() + "tmp");

        try (FileChannel fileChannel = FileChannel.open(
                tmp,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {
            ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            while (iterators.hasNext()) {
                Record record = iterators.next();

                writeInt(record.getKey(), fileChannel, size);
                writeInt(record.getValue(), fileChannel, size);
            }
            fileChannel.force(false);
        }


        Files.deleteIfExists(dir);
        Files.move(tmp, dir, StandardCopyOption.ATOMIC_MOVE);

        return new SSTable(dir);
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

    private static void writeInt(ByteBuffer value, WritableByteChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        tmp.putInt(value.remaining());
        tmp.position(0);
        channel.write(tmp);
        channel.write(value);
    }

    void close() throws IOException {
        if (mappedByteBuffer != null) {
            clean();
        }
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
