package ru.mail.polis.lsm.sachuk.ilya;

import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
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
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class SSTable {

    private static final String SAVE_FILE = "SSTABLE";
    private static final String INDEX_FILE = "INDEX";
    private static final String TMP_FILE = "TMP";
    private static final String NULL_VALUE = "NULL_VALUE";

    private final Path savePath;
    private final Path indexPath;
    private final List<Long> indexList = new ArrayList<>();

    private MappedByteBuffer mappedByteBuffer;
    private MappedByteBuffer indexByteBuffer;


    SSTable(Path savePath, Path indexPath) throws IOException {
        this.savePath = savePath;
        this.indexPath = indexPath;

        restoreStorage();
    }

    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) throws IOException {
        return new SSTableIterator(binarySearchKey(indexList, fromKey), toKey);
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
                ByteBuffer longSize = ByteBuffer.allocate(Long.BYTES);

                while (iterators.hasNext()) {

                    //indexPath
                    long indexPositionToRead = saveFileChannel.position();

//                    ByteBuffer offset = ByteBuffer.allocate(Long.BYTES).putLong(indexPositionToRead);
                    ByteBuffer offset = ByteBuffer.wrap(ByteBuffer.allocate(Long.BYTES).putLong(indexPositionToRead).array());
//                    offset.position(0);

                    //offset
                    indexFileChanel.write(offset);
//                    writeInt(offset, indexFileChanel, longSize);

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
            clean(mappedByteBuffer);
        }

        if (indexByteBuffer != null) {
            clean(indexByteBuffer);
            indexByteBuffer.clear();
            indexList.clear();
        }

    }

    private int binarySearchKey(List<Long> indexList, ByteBuffer keyToFind) throws IOException {

        if (keyToFind == null) {
            return 0;
        }

        int start = 0;
        int end = indexList.size() - 1;

        String keyStringToFind = byteBufferToString(keyToFind);

        int positionToRead = 0;

        while (start <= end) {

            int middle = (start + end) / 2;

            positionToRead = indexList.get(middle).intValue();

            mappedByteBuffer.position(positionToRead);

            ByteBuffer key = readFromFile(mappedByteBuffer);

            String keyString = byteBufferToString(key);

            if (keyStringToFind.compareTo(keyString) == 0) {
                return positionToRead;
            } else if (keyStringToFind.compareTo(keyString) > 0) {
                start = middle + 1;
            } else {
                end = middle - 1;
            }

        }
        return positionToRead;
    }

    private void restoreStorage() throws IOException {
        try (FileChannel saveFileChannel = FileChannel.open(savePath, StandardOpenOption.READ)) {
            try (FileChannel indexFileChannel = FileChannel.open(indexPath, StandardOpenOption.READ)) {

                mappedByteBuffer = saveFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, saveFileChannel.size());
                indexByteBuffer = indexFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, indexFileChannel.size());

                while (indexByteBuffer.hasRemaining()) {

//                    int length = indexByteBuffer.getInt();
//                    ByteBuffer byteBuffer = indexByteBuffer.slice().limit(length).asReadOnlyBuffer();
//                    indexByteBuffer.position(indexByteBuffer.position() + length);
//
//                    indexList.add(byteBuffer);
                    indexList.add(indexByteBuffer.getLong());
                }

//                while (mappedByteBuffer.hasRemaining()) {
//                    ByteBuffer keyByteBuffer = readFromFile(mappedByteBuffer);
//                    ByteBuffer valueByteBuffer = readFromFile(mappedByteBuffer);
//
//                    Record record;
//                    if (StandardCharsets.UTF_8.newDecoder().decode(valueByteBuffer).toString().compareTo(NULL_VALUE) == 0) {
//                        record = Record.tombstone(keyByteBuffer);
//                    } else {
//                        valueByteBuffer.position(0);
//                        record = Record.of(keyByteBuffer, valueByteBuffer);
//                    }
//                }
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

    private void clean(MappedByteBuffer mappedByteBuffer) throws IOException {
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

    private String byteBufferToString(ByteBuffer buffer) {
        return StandardCharsets.UTF_8.decode(buffer).toString();
    }

    class SSTableIterator implements Iterator<Record> {

        private int positionToStartRead;
        private ByteBuffer keyToRead;
        private String keyToReadString;
        private boolean readToEnd = false;

        SSTableIterator(int positionToStartRead, ByteBuffer keyToRead) {
            this.positionToStartRead = positionToStartRead;
            this.keyToRead = keyToRead;
            this.keyToReadString = keyToRead == null ? null : byteBufferToString(keyToRead);

            readToEnd = keyToRead == null;

            mappedByteBuffer.position(positionToStartRead);


        }

        @Override
        public boolean hasNext() {
            try {
                if (readToEnd) {
                    return mappedByteBuffer.hasRemaining();
                }

                return mappedByteBuffer.hasRemaining() && getNextKey().compareTo(keyToReadString) < 0;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public Record next() {
            Record record;
            try {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                ByteBuffer key = readFromFile(mappedByteBuffer);
                ByteBuffer value = readFromFile(mappedByteBuffer);

                if (byteBufferToString(value).compareTo(NULL_VALUE) == 0) {
                    record = Record.tombstone(key);
                } else {
                    value.position(0);
                    record = Record.of(key, value);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return record;
        }

        private String getNextKey() throws IOException {
            int currentPos = mappedByteBuffer.position();

            ByteBuffer key = readFromFile(mappedByteBuffer);
            String stringKey = byteBufferToString(key);
            mappedByteBuffer.position(currentPos);

            return stringKey;
        }
    }
}
