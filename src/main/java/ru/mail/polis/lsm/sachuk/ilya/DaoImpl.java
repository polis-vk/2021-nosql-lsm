package ru.mail.polis.lsm.sachuk.ilya;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DaoImpl implements DAO {

    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();

    private static final String FILE_NAME = "data";
    private final Path path;

    public DaoImpl(Path dirPath) {
        this.path = dirPath.resolve(Paths.get(FILE_NAME));
        restoreStorage();
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return map(fromKey, toKey).values().iterator();
    }

    @Override
    public void upsert(Record record) {

        if (record.getValue() != null) {
            storage.put(record.getKey(), record);
        } else {
            storage.remove(record.getKey());
        }

    }

    @Override
    public void close() {
        save();
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

    private void save() {
        try (BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(Files.newOutputStream(path))) {
            storage.forEach((key, value) -> {
                try {
                    writeToFile(bufferedOutputStream, key);
                    writeToFile(bufferedOutputStream, value.getValue());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void restoreStorage() {
        if (Files.exists(path)) {
            try (BufferedInputStream bufferedInputStream = new BufferedInputStream(Files.newInputStream(path))) {
                while (bufferedInputStream.available() > 0) {

                    ByteBuffer keyByteBuffer = readFromFile(bufferedInputStream);

                    ByteBuffer valueByteBuffer = readFromFile(bufferedInputStream);

                    storage.put(keyByteBuffer, Record.of(keyByteBuffer, valueByteBuffer));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private ByteBuffer readFromFile(BufferedInputStream bufferedInputStream) throws IOException {
        int length = bufferedInputStream.read();
        return ByteBuffer.wrap(bufferedInputStream.readNBytes(length));
    }

    private void writeToFile(BufferedOutputStream bufferedOutputStream, ByteBuffer byteBuffer) throws IOException {
        int length = byteBuffer.remaining();

        byte[] bytes = new byte[length];
        byteBuffer.get(bytes);

        bufferedOutputStream.write(length);
        bufferedOutputStream.write(bytes);
    }
}
