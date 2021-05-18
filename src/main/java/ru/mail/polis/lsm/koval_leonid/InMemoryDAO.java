package ru.mail.polis.lsm.koval_leonid;

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
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDAO implements DAO {

    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    private static final String SAVE_FILE_NAME = "leonid.bat";
    private final DAOConfig config;

    public InMemoryDAO(DAOConfig config) throws IOException {
        this.config = config;

        final Path path = config.getDir().resolve(SAVE_FILE_NAME);
        if (!Files.exists(path)) {
            return;
        }

        try (BufferedInputStream inputStream = new BufferedInputStream(Files.newInputStream(path))) {
            while (inputStream.available() > 0) {
                final ByteBuffer key = readFile(inputStream);
                final ByteBuffer value = readFile(inputStream);
                storage.put(key, Record.of(key, value));
            }
        } catch (IOException e) {
            throw new IOException(e);
        }
    }


    private ByteBuffer readFile(BufferedInputStream stream) throws IOException {
        final int length = stream.read();
        return ByteBuffer.wrap(stream.readNBytes(length));
    }


    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return map(fromKey, toKey).values().iterator();
    }

    @Override
    public void upsert(Record record) {
        if (record.getValue() == null) {
            storage.remove(record.getKey());
        } else {
            storage.put(record.getKey(), record);
        }
    }

    @Override
    public void close() throws IOException {
        try (BufferedOutputStream outputStream = new BufferedOutputStream(Files.newOutputStream(config.getDir().resolve(SAVE_FILE_NAME)))) {
            for (final Map.Entry<ByteBuffer, Record> temp : storage.entrySet()) {
                writeFile(outputStream, temp.getKey());
                writeFile(outputStream, temp.getValue().getValue());
            }
        } catch (IOException e) {
            throw new IOException(e);
        }
    }

    private void writeFile(BufferedOutputStream outputStream, ByteBuffer byteBuffer) throws IOException {
        final int length = byteBuffer.remaining();
        final byte[] bytes = new byte[length];
        byteBuffer.get(bytes);
        outputStream.write(length);
        outputStream.write(bytes);
    }


    private SortedMap<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null)
            return storage;

        if (fromKey == null)
            return storage.headMap(toKey);

        if (toKey == null)
            return storage.tailMap(fromKey);

        return storage.subMap(fromKey, toKey);
    }
}
