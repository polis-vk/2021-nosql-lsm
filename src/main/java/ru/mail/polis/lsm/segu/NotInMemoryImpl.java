package ru.mail.polis.lsm.segu;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class NotInMemoryImpl implements DAO {

    private final Logger logger = LoggerFactory.getLogger(getClass().getName());

    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();

    private final DAOConfig config;
    private static final String FILE_NAME = "data.dat";
    private Path filePath;

    public NotInMemoryImpl(DAOConfig config) {
        this.config = config;
        try {
            initStorage();
        } catch (IOException e) {
            logger.error("Failed to init file");
        }
    }


    private void initStorage() throws IOException {
        filePath = config.getDir().resolve(FILE_NAME);
        if (Files.exists(filePath)) {
            try (FileChannel fileChannel = FileChannel.open(filePath, StandardOpenOption.READ)) {
                ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
                while (fileChannel.read(size) > 0) {
                    ByteBuffer key = readValue(fileChannel, size);
                    fileChannel.read(size.flip());
                    ByteBuffer value = readValue(fileChannel, size);
                    storage.put(key, Record.of(key, value));
                }
            }
        }
    }

    @Override
    public Iterator<Record> range(@Nullable final ByteBuffer fromKey, @Nullable final ByteBuffer toKey) {
        return map(fromKey, toKey).values().stream()
                .filter(record -> record.getValue() != null)
                .iterator();
    }

    @Override
    public void upsert(final Record record) {
        storage.put(record.getKey(), record);
    }

    @Override
    public void close() throws IOException {
        try (FileChannel fileChannel = FileChannel.open(filePath, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            for (Record record : storage.values()) {
                if (!isTombstone(record)) {
                    writeValue(fileChannel, record.getKey(), size);
                    writeValue(fileChannel, record.getValue(), size);
                }
            }
        }
    }

    private static void writeValue(FileChannel fileChannel, ByteBuffer value, ByteBuffer size) throws IOException {
        size.position(0);
        size.putInt(value.remaining());
        size.position(0);
        fileChannel.write(size);
        fileChannel.write(value);
    }

    private static ByteBuffer readValue(FileChannel fileChannel, ByteBuffer size) throws IOException {
        size.flip();
        ByteBuffer value = ByteBuffer.allocate(size.getInt());
        fileChannel.read(value);
        return value.flip();
    }

    private SortedMap<ByteBuffer, Record> map(@Nullable final ByteBuffer fromKey, @Nullable final ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return storage;
        }
        if (fromKey == null) {
            return storage.headMap(toKey);
        }
        if (toKey == null) {
            return storage.tailMap(fromKey);
        }
        return storage.subMap(fromKey, toKey);
    }

    public boolean isTombstone(Record record) {
        return record.getValue() == null;
    }
}
