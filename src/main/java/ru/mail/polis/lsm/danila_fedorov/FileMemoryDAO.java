package ru.mail.polis.lsm.danila_fedorov;

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
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

public class FileMemoryDAO implements DAO {

    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    private final DAOConfig config;

    private static final String SAVED_FILE_NAME = "Data";
    private static final String NEW_FILE_NAME = "Temp";

    public FileMemoryDAO(final DAOConfig config) throws IOException {
        this.config = config;
        restore();
    }

    @Override
    public Iterator<Record> range(@Nullable final ByteBuffer fromKey, @Nullable final ByteBuffer toKey) {
        return getStreamOfValidValues(fromKey, toKey).iterator();
    }

    @Override
    public void upsert(final Record record) {
        storage.put(record.getKey(), record);
    }

    @Override
    public void close() throws IOException {
        save();
    }

    private SortedMap<ByteBuffer, Record> map(@Nullable final ByteBuffer fromKey, @Nullable final ByteBuffer toKey) {
        if (fromKey == null) {
            if (toKey == null) {
                return storage;
            }

            return storage.headMap(toKey);
        }

        if (toKey == null) {
            return storage.tailMap(fromKey);
        }

        return storage.subMap(fromKey, toKey);
    }

    private Stream<Record> getStreamOfValidValues(
            @Nullable final ByteBuffer fromKey,
            @Nullable final ByteBuffer toKey
    ) {
        return map(fromKey, toKey).values()
                .stream()
                .filter(record -> !record.isTombstone());
    }

    private void save() throws IOException {
        final Path tempPath = config.getDir().resolve(NEW_FILE_NAME);

        try (BufferedOutputStream stream = new BufferedOutputStream(Files.newOutputStream(tempPath))) {
            var it = getStreamOfValidValues(null, null).iterator();

            while (it.hasNext()) {
                var value = it.next();
                write(stream, value.getKey());
                write(stream, value.getValue());
            }
        }

        final Path mainPath = tempPath.resolveSibling(SAVED_FILE_NAME);

        Files.copy(tempPath, mainPath, StandardCopyOption.REPLACE_EXISTING);
    }

    private void restore() throws IOException {
        final Path path = config.getDir().resolve(SAVED_FILE_NAME);
        if (Files.exists(path)) {
            try (BufferedInputStream stream = new BufferedInputStream(Files.newInputStream(path))) {
                while (stream.available() > 0) {
                    final ByteBuffer key = read(stream);
                    final ByteBuffer value = read(stream);

                    storage.put(key, Record.of(key, value));
                }
            }
        }
    }

    private void write(final BufferedOutputStream stream, final ByteBuffer data) throws IOException {
        final int length = data.remaining();
        final byte[] bytes = new byte[length];
        data.get(bytes);

        stream.write(length);
        stream.write(bytes);
    }

    private ByteBuffer read(final BufferedInputStream stream) throws IOException {
        final int length = stream.read();
        return ByteBuffer.wrap(stream.readNBytes(length));
    }
}
