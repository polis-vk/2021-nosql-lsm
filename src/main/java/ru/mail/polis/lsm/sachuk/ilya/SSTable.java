package ru.mail.polis.lsm.sachuk.ilya;

import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.File;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class SSTable {

    private static final String SAVE_FILE = "SSTABLE";
    private static final String SAVE_FILE_END = ".save";

    private static final String INDEX_FILE = "INDEX";
    private static final String INDEX_FILE_END = ".index";

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

        if (fromKey != null && toKey != null && byteBufferToString(fromKey).compareTo(byteBufferToString(toKey)) == 0) {
            return Collections.emptyIterator();
        }

        return new SSTableIterator(binarySearchKey(indexList, fromKey), toKey);
    }

    static List<SSTable> loadFromDir(Path dir) throws IOException {

        List<SSTable> listSSTables = new ArrayList<>();

        Iterator<Path> savePaths = getPathIterator(dir, SAVE_FILE_END);
        Iterator<Path> indexPaths = getPathIterator(dir, INDEX_FILE_END);

        while (savePaths.hasNext() && indexPaths.hasNext()) {
            Path savePath = savePaths.next();
            Path indexPath = indexPaths.next();

            listSSTables.add(new SSTable(savePath, indexPath));
        }

        return listSSTables;
    }

    static SSTable save(Iterator<Record> iterators, Path dir, int fileNumber) throws IOException {

        Path savePath = dir.resolve(SAVE_FILE + fileNumber + SAVE_FILE_END);
        Path indexPath = dir.resolve(INDEX_FILE + fileNumber + INDEX_FILE_END);

        Path tmpSavePath = dir.resolve(SAVE_FILE + "_" + TMP_FILE);
        Path tmpIndexPath = dir.resolve(INDEX_FILE + "_" + TMP_FILE);

        Files.deleteIfExists(tmpSavePath);
        Files.deleteIfExists(tmpIndexPath);

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

                while (iterators.hasNext()) {
                    long indexPositionToRead = saveFileChannel.position();
                    ByteBuffer offset = ByteBuffer.wrap(
                            ByteBuffer.allocate(Long.BYTES).putLong(indexPositionToRead).array()
                    );

                    indexFileChanel.write(offset);


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

        int positionToRead;

        int middle = (start + end) / 2;

        while (start <= end) {

            middle = (start + end) / 2;

            positionToRead = indexList.get(middle).intValue();

            mappedByteBuffer.position(positionToRead);

            ByteBuffer key = readFromFile(mappedByteBuffer);

            String keyString = byteBufferToString(key);

            if (keyStringToFind.compareTo(keyString) == 0) {
                return positionToRead;
            } else if (keyStringToFind.compareTo(keyString) > 0) {
                start = middle + 1;

                if (start > end && keyStringToFind.compareTo(keyString) > 0) {

                    if (start == indexList.size()) {
                        return -1;
                    }

                    if (start < indexList.size()) {
                        return start;
                    }
                }
            } else {
                end = middle - 1;
            }

        }
        return indexList.get(middle).intValue();
    }

    private void restoreStorage() throws IOException {
        try (FileChannel saveFileChannel = FileChannel.open(savePath, StandardOpenOption.READ)) {
            try (FileChannel indexFileChannel = FileChannel.open(indexPath, StandardOpenOption.READ)) {

                mappedByteBuffer = saveFileChannel.map(
                        FileChannel.MapMode.READ_ONLY,
                        0,
                        saveFileChannel.size()
                );
                indexByteBuffer = indexFileChannel.map(
                        FileChannel.MapMode.READ_ONLY,
                        0,
                        indexFileChannel.size()
                );

                while (indexByteBuffer.hasRemaining()) {
                    indexList.add(indexByteBuffer.getLong());
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
        buffer.position(0);
        String value = StandardCharsets.UTF_8.decode(buffer).toString();
        buffer.position(0);
        return value;
    }

    private static Iterator<Path> getPathIterator(Path dir, String pathEnd) throws IOException {
        Iterator<Path> paths;
        try (Stream<Path> streamPaths = Files.walk(Paths.get(dir.toUri()))) {
            paths = streamPaths.filter(path -> path.toString().endsWith(pathEnd))
                    .collect(Collectors.toList())
                    .stream()
                    .sorted(Comparator.comparing(o -> getFileNumber(o, pathEnd)))
                    .collect(Collectors.toList())
                    .iterator();
        }
        return paths;
    }

    private static Integer getFileNumber(Path path, String endFile) {
        String stringPath = path.toString();

        int lastSlash = 0;

        for (int i = 0; i < stringPath.length(); i++) {
            if (stringPath.charAt(i) == File.separatorChar) {
                lastSlash = i;
            }
        }

        stringPath = stringPath.substring(lastSlash + 1);

        int firstNumberIndex = 0;

        for (int i = 0; i < stringPath.length(); i++) {
            if (Character.isDigit(stringPath.charAt(i))) {
                firstNumberIndex = i;
                break;
            }
        }

        return Integer.parseInt(stringPath.substring(firstNumberIndex, stringPath.length() - endFile.length()));
    }

    class SSTableIterator implements Iterator<Record> {
        private final String keyToReadString;
        private final boolean readToEnd;

        SSTableIterator(int positionToStartRead, ByteBuffer keyToRead) {
            this.keyToReadString = keyToRead == null ? null : byteBufferToString(keyToRead);

            readToEnd = keyToRead == null;

            if (positionToStartRead == -1) {
                mappedByteBuffer.position(mappedByteBuffer.limit());
            } else {
                mappedByteBuffer.position(positionToStartRead);
            }
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
