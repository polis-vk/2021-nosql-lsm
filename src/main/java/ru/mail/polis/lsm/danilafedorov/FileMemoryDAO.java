package ru.mail.polis.lsm.danilafedorov;

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
    private final Path backupFilePath;
    private final Path dataFilePath;
    private final Path tempFilePath;

    private static final String BACKUP_FILE_NAME = "Backup";
    private static final String DATA_FILE_NAME = "Data";
    private static final String TEMP_FILE_NAME = "Temp";

    /**
     * Class constructor identifying directory of DB location.
     *
     * @param config contains directory path of DB location
     * @throws IOException If an input exception occured
     */
    public FileMemoryDAO(final DAOConfig config) throws IOException {
        Path dir = config.getDir();
        backupFilePath = dir.resolve(BACKUP_FILE_NAME);
        dataFilePath = dir.resolve(DATA_FILE_NAME);
        tempFilePath = dir.resolve(TEMP_FILE_NAME);
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
        try (BufferedOutputStream stream = new BufferedOutputStream(Files.newOutputStream(tempFilePath))) {
            Iterator<Record> it = getStreamOfValidValues(null, null).iterator();

            while (it.hasNext()) {
                Record value = it.next();
                write(stream, value.getKey());
                write(stream, value.getValue());
            }
        }

        if (Files.exists(dataFilePath)) {
            Files.copy(dataFilePath, backupFilePath, StandardCopyOption.REPLACE_EXISTING);
        }
        Files.move(tempFilePath, dataFilePath, StandardCopyOption.REPLACE_EXISTING);
    }

    private void restore() throws IOException {
        Path path = dataFilePath;
        if (!Files.exists(path)) {
            path = backupFilePath;
        }

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
