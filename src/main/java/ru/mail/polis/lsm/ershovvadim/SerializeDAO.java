package ru.mail.polis.lsm.ershovvadim;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * implementation Serialize DAO.
 */
public class SerializeDAO implements DAO {
    private static final String FILE_TO_SAVE = "DAO_FILE_TO_SAVE.dat";
    private final DAOConfig config;
    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();

    /**
     * Constructor SerializeDAO object, deserialize data from file
     *
     * @param config DAOConfig
     */
    public SerializeDAO(DAOConfig config) {
        this.config = config;
        Path resolve = config.getDir().resolve(FILE_TO_SAVE);
        if (!Files.exists(resolve)) {
            return;
        }
        try (BufferedInputStream inputStream =
                     new BufferedInputStream(Files.newInputStream(resolve))) {
            ByteBuffer key;
            ByteBuffer value;
            while (inputStream.available() > 0) {
                key = read(inputStream);
                value = read(inputStream);
                storage.put(key, Record.of(key, value));
            }
        } catch (IOException e) {
            e.getStackTrace();
        }
    }

    private ByteBuffer read(BufferedInputStream inputStream) throws IOException {
        int len = inputStream.read();
        return ByteBuffer.wrap(inputStream.readNBytes(len));
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return pruneMap(fromKey, toKey).values().stream()
                .filter(recordIter -> recordIter.getValue() != null)
                .iterator();
    }

    private SortedMap<ByteBuffer, Record> pruneMap(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return storage;
        } else if (fromKey == null) {
            return storage.headMap(toKey);
        } else if (toKey == null) {
            return storage.tailMap(fromKey);
        }
        return storage.subMap(fromKey, toKey);
    }

    @Override
    public void upsert(Record recordUpsert) {
        storage.put(recordUpsert.getKey(), recordUpsert);
    }

    @Override
    public void close() throws IOException {
        Path file = config.getDir().resolve(FILE_TO_SAVE);
        Files.deleteIfExists(file);
        Files.createFile(file);
        try (BufferedOutputStream outputStream =
                     new BufferedOutputStream(Files.newOutputStream(file))) {
            Iterator<Record> cleanStorage = range(null, null);
            Record next;
            while (cleanStorage.hasNext()) {
                next = cleanStorage.next();
                write(outputStream, next.getKey());
                write(outputStream, next.getValue());
            }
        }
    }

    private void write(BufferedOutputStream outputStream, ByteBuffer current) throws IOException {
        int length = current.remaining();
        byte[] bytes = new byte[length];
        current.get(bytes);
        outputStream.write(length);
        outputStream.write(bytes);
    }
}
