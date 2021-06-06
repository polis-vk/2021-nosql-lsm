package ru.mail.polis.lsm.sachuk.ilya;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;

public class DaoImpl implements DAO {

    private static final String SAVE_FILE_NAME = "save";
    private static final String TMP_FILE_NAME = "tmp";

    private final Path savePath;
    private final Path tmpPath;
    private final SortedMap<ByteBuffer, Record> memoryStorage = new ConcurrentSkipListMap<>();
    private final ConcurrentLinkedDeque<SSTable> tables = new ConcurrentLinkedDeque<>();


    private MappedByteBuffer mappedByteBuffer;

    /**
     * Constructor that initialize path and restore storage.
     *
     * @param dirPath path to the directory in which will be created file
     * @throws IOException is thrown when an I/O error occurs.
     */
    public DaoImpl(Path dirPath) throws IOException {
//        this.savePath = dirPath.resolve(Paths.get(SAVE_FILE_NAME));
//        this.tmpPath = dirPath.resolve(Paths.get(TMP_FILE_NAME));
//
//        if (!Files.exists(savePath)) {
//            if (Files.exists(tmpPath)) {
//                Files.move(tmpPath, savePath, StandardCopyOption.ATOMIC_MOVE);
//            } else {
//                mappedByteBuffer = null;
//                return;
//            }
//
//        }
//        restoreStorage();

        this.config = config;
        tables.add(SSTable.loadFromDir(config.getDir()));
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        synchronized (this) {
            Iterator<Record> sstableRanges = sstableRanges(fromKey, toKey);
            Iterator<Record> memoryRange = map(fromKey, toKey).values().iterator();
            return mergeTwo(sstableRange, memoryRange);
        }

        return map(fromKey, toKey).values().iterator();
    }

    @Override
    public void upsert(Record record) {

        synchronized (this) {
            memoryConsuption += sizeOf(record);
            if (memoryConsuption > memoryList) {
                try {
                    flush();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }

        if (record.getValue() == null) {
            memoryStorage.remove(record.getKey());
        } else {
            memoryStorage.put(record.getKey(), record);
        }

    }

    @Override
    public void close() throws IOException {
//        save();
//
//        if (mappedByteBuffer != null) {
//            clean();
//        }
//
//        Files.deleteIfExists(savePath);
//        Files.move(tmpPath, savePath, StandardCopyOption.ATOMIC_MOVE);
        flush();
    }

    private Map<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {

        if (fromKey == null && toKey == null) {
            return memoryStorage;
        } else if (fromKey == null) {
            return memoryStorage.headMap(toKey);
        } else if (toKey == null) {
            return memoryStorage.tailMap(fromKey);
        } else {
            return memoryStorage.subMap(fromKey, toKey);
        }
    }

    private void flush() throws IOException {
        SSTable ssTable = SSTable.save(memoryStorage.values().iterator(), ...);
        tables.add(ssTable);
        memoryStorage.clear();
    }

    private Iterator<Record> sstableRanges(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(sstable.size());

        iterators.forEach(() -> sstable.range(fromKey, toKey));

        return merge(iterators);
    }

    private void save() throws IOException {

        try (FileChannel fileChannel = FileChannel.open(
                tmpPath,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {

            for (final Map.Entry<ByteBuffer, Record> byteBufferRecordEntry : memoryStorage.entrySet()) {
                writeToFile(fileChannel, byteBufferRecordEntry.getKey());
                writeToFile(fileChannel, byteBufferRecordEntry.getValue().getValue());
            }
            fileChannel.force(false);
        }
    }

    private void restoreStorage() throws IOException {
        if (Files.exists(savePath)) {
            try (FileChannel fileChannel = FileChannel.open(savePath, StandardOpenOption.READ)) {

                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());

                while (mappedByteBuffer.hasRemaining()) {
                    ByteBuffer keyByteBuffer = readFromFile(mappedByteBuffer);
                    ByteBuffer valueByteBuffer = readFromFile(mappedByteBuffer);

                    memoryStorage.put(keyByteBuffer, Record.of(keyByteBuffer, valueByteBuffer));
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
}
