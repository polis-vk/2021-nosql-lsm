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
import java.nio.charset.StandardCharsets;
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

    private static final String SAVE_FILE = "SSTABLE";
    private static final String INDEX_FILE = "INDEX";
    private static final String TMP_FILE = "TMP";
    private static final String NULL_VALUE = "NULL_VALUE";

    private final Path savePath;
    private final Path indexPath;
    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    private MappedByteBuffer mappedByteBuffer;
    private List<Long> index;


    SSTable(Path savePath, Path indexPath) throws IOException {
        this.savePath = savePath;
        this.indexPath = indexPath;

        restoreStorage();
    }

    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return map(fromKey, toKey).values().iterator();
    }

    static List<SSTable> loadFromDir(Path dir) throws IOException {

        List<SSTable> listSSTables = new ArrayList<>();

        Iterator<Path> savePaths;
        Iterator<Path> indexPaths;


        try (Stream<Path> streamPaths = Files.walk(Paths.get(dir.toUri()))) {
            savePaths = streamPaths.filter(path -> path.toString().endsWith(".save")).collect(Collectors.toList()).iterator();
        }

        try (Stream<Path> streamPaths = Files.walk(Paths.get(dir.toUri()))) {
            indexPaths = streamPaths.filter(path -> path.toString().endsWith(".index")).collect(Collectors.toList()).iterator();
        }

        while (savePaths.hasNext() && indexPaths.hasNext()) {
            Path savePath = savePaths.next();
            Path indexPath = indexPaths.next();

            listSSTables.add(new SSTable(savePath, indexPath));
        }

        return listSSTables;
    }

    static SSTable save(Iterator<Record> iterators, Path dir, int fileNumber) throws IOException {

        Path savePath = dir.resolve(SAVE_FILE + fileNumber + ".save");
        Path indexPath = dir.resolve(INDEX_FILE + fileNumber + ".index");

        Path tmpSavePath = dir.resolve(SAVE_FILE + "_" + TMP_FILE);
        Path tmpIndexPath = dir.resolve(INDEX_FILE + "_" + TMP_FILE);

        try (FileChannel saveFileChannel = FileChannel.open(
                tmpSavePath,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            try (FileChannel indexFileChanel = FileChannel.open(
                    tmpIndexPath, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {

                ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
                ByteBuffer longSize = ByteBuffer.allocate(Integer.BYTES);

                while (iterators.hasNext()) {

                    //indexPath
                    long indexPositionToRead = saveFileChannel.position();

                    ByteBuffer offset = ByteBuffer.allocate(Long.BYTES).putLong(indexPositionToRead);
                    offset.position(0);

                    //offset
                    writeInt(offset, indexFileChanel, longSize);

                    //savePath
                    Record record = iterators.next();

                    ByteBuffer value = record.getValue() != null
                            ? record.getValue()
                            : ByteBuffer.wrap(NULL_VALUE.getBytes(StandardCharsets.UTF_8));

                    writeInt(record.getKey(), saveFileChannel, size);
                    writeInt(value, saveFileChannel, size);
                }

                saveFileChannel.force(false);
                indexFileChanel.force(false);
            }
        }


        Files.deleteIfExists(savePath);
        Files.deleteIfExists(indexPath);

        Files.move(tmpSavePath, savePath, StandardCopyOption.ATOMIC_MOVE);
        Files.move(tmpIndexPath, indexPath, StandardCopyOption.ATOMIC_MOVE);

        return new SSTable(savePath, indexPath);
    }

    void close() throws IOException {
        if (mappedByteBuffer != null) {
            clean();
        }
    }

    private void restoreStorage() throws IOException {
        try (FileChannel fileChannel = FileChannel.open(savePath, StandardOpenOption.READ)) {

            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());

            while (mappedByteBuffer.hasRemaining()) {
                ByteBuffer keyByteBuffer = readFromFile(mappedByteBuffer);
                ByteBuffer valueByteBuffer = readFromFile(mappedByteBuffer);

                Record record;
                if (StandardCharsets.UTF_8.newDecoder().decode(valueByteBuffer).toString().compareTo(NULL_VALUE) == 0) {
                    record = Record.tombstone(keyByteBuffer);
                } else {
                    valueByteBuffer.position(0);
                    record = Record.of(keyByteBuffer, valueByteBuffer);
                }

                storage.put(keyByteBuffer, record);
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
}
