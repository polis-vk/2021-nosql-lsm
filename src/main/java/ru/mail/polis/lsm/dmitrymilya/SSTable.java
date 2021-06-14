package ru.mail.polis.lsm.dmitrymilya;

import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

class SSTable {
    private static final Method CLEAN;

    static {
        try {
            Class<?> clazz = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = clazz.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    private final MappedByteBuffer mappedByteBuffer;

    SSTable(Path file) throws IOException {

        try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ)) {
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
        }
    }

    static List<SSTable> loadFromDir(Path dir) throws IOException {
        List<Path> files;

        try (Stream<Path> walk = Files.walk(dir)) {
            files = walk.filter(Files::isRegularFile).collect(Collectors.toList());
        }

        if (files.isEmpty()) {
            return new ArrayList<>();
        }

        List<SSTable> tables = new ArrayList<>();
        for (Path file : files) {
            tables.add(new SSTable(file));
        }

        return tables;
    }

    static SSTable write(Iterator<Record> records, Path saveFileName) throws IOException {
        Path tmpFileName = saveFileName.getParent().resolve(saveFileName.getFileName() + "_tmp");
        try (FileChannel fileChannel = FileChannel.open(
                tmpFileName,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {
            final ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            while (records.hasNext()) {
                Record record = records.next();
                writeInt(record.getKey(), fileChannel, size);
                writeInt(record.getValue(), fileChannel, size);
            }
            fileChannel.force(false);
        }

        Files.deleteIfExists(saveFileName);
        Files.move(tmpFileName, saveFileName, StandardCopyOption.ATOMIC_MOVE);

        return new SSTable(saveFileName);
    }

    private static void writeInt(ByteBuffer value, WritableByteChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        if (value != null) {
            tmp.putInt(value.remaining());
        } else {
            tmp.putInt(-1);
        }
        tmp.position(0);
        channel.write(tmp);
        if (value != null) {
            channel.write(value);
        }
    }

    public void cleanMappedByteBuffer() {
        try {
            CLEAN.invoke(null, mappedByteBuffer);
        } catch (InvocationTargetException | IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return new SSTableIterator(mappedByteBuffer);
        }
        if (fromKey == null) {
            return new SSTableIterator(mappedByteBuffer.limit(findKeyPosition(toKey)).asReadOnlyBuffer());
        }
        if (toKey == null) {
            return new SSTableIterator(mappedByteBuffer.position(findKeyPosition(fromKey)).slice().asReadOnlyBuffer());
        }

        int fromKeyPosition = findKeyPosition(fromKey);
        int toKeyPosition = findKeyPosition(toKey);
        return new SSTableIterator(mappedByteBuffer.position(fromKeyPosition).slice().limit(toKeyPosition - fromKeyPosition).asReadOnlyBuffer());
    }

    private int findKeyPosition(ByteBuffer key) {
        ByteBuffer mappedByteBufferDuplicate = mappedByteBuffer.duplicate();

        while (mappedByteBufferDuplicate.hasRemaining()) {
            int size = mappedByteBufferDuplicate.getInt();
            int curPos = mappedByteBufferDuplicate.position();
            ByteBuffer current = mappedByteBufferDuplicate.slice();
            if (size < current.capacity()) {
                current.limit(size);
            }
            if (key.compareTo(current) == 0) {
                return mappedByteBufferDuplicate.position();
            }

            mappedByteBufferDuplicate.position(curPos + size);
        }

        return mappedByteBufferDuplicate.position();
    }

    static class SSTableIterator implements Iterator<Record> {

        final ByteBuffer byteBuffer;
        Record nextRecord;

        SSTableIterator(ByteBuffer byteBuffer) {
            this.byteBuffer = byteBuffer;
            if (byteBuffer.hasRemaining()) {
                getNextRecord();
            }
        }

        @Override
        public boolean hasNext() {
            return nextRecord != null;
        }

        @Override
        public Record next() {
            Record toReturn = nextRecord;
            if (byteBuffer.hasRemaining()) {
                getNextRecord();
            } else {
                nextRecord = null;
            }
            while (nextRecord != null && nextRecord.isTombstone() && byteBuffer.hasRemaining()) {
                getNextRecord();
            }
            return toReturn;
        }

        private void getNextRecord() {
            int keySize = byteBuffer.getInt();
            ByteBuffer key = byteBuffer.slice();
            if (keySize < key.capacity()) {
                key.limit(keySize);
            }
            key = key.asReadOnlyBuffer();

            byteBuffer.position(Math.min(byteBuffer.position() + keySize, byteBuffer.limit()));

            int valueSize = byteBuffer.getInt();
            ByteBuffer value;
            if (valueSize != -1) {
                value = byteBuffer.slice();
                if (valueSize < value.capacity()) {
                    value.limit(valueSize);
                }
                value = value.asReadOnlyBuffer();

                byteBuffer.position(Math.min(byteBuffer.position() + valueSize, byteBuffer.limit()));

                nextRecord = Record.of(key, value);
            } else {
                nextRecord = Record.tombstone(key);
            }
        }
    }

}
