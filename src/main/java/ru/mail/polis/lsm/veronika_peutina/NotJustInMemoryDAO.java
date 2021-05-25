package ru.mail.polis.lsm.veronika_peutina;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class NotJustInMemoryDAO implements DAO {
    private final String SAVE_FILE_NAME = "save.dat";
    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    private final DAOConfig config;

    public NotJustInMemoryDAO(DAOConfig config) {
        this.config = config;
        Path pathFile = config.getDir().resolve(SAVE_FILE_NAME);
        if (!Files.exists(pathFile)) {
            return;
        }

        try (FileChannel fileChannel = FileChannel.open(pathFile, StandardOpenOption.READ)) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
            while (fileChannel.position() < fileChannel.size()) {
                Record record = readRecord(fileChannel, byteBuffer);
                storage.put(record.getKey(), record);
            }

        } catch (IOException e) {
            System.out.println("Error with read File");
        }
    }



    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return getSubMap(fromKey, toKey).values().iterator();
    }

    private SortedMap<ByteBuffer, Record> getSubMap(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
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

    @Override
    public void upsert(Record record) {
        if (record.getKey() != null) {
            if (record.getValue() != null) {
                storage.put(record.getKey(), record);
            } else {
                storage.remove(record.getKey());
            }
        }
    }

    @Override
    public void close() throws IOException {
        Files.deleteIfExists(config.getDir().resolve(SAVE_FILE_NAME));
        Path pathFile = config.getDir().resolve(SAVE_FILE_NAME);
        try (FileChannel fileChannel = FileChannel.open(pathFile,
                            StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
            for (final Record record : storage.values()) {
                writeRecord(record, fileChannel, byteBuffer);
            }
        } catch (IOException e) {
            System.out.println("Error with write in File");
        }
    }


    private void writeRecord(Record record, WritableByteChannel channel, ByteBuffer tmp) throws IOException {
        write(record.getKey(), channel, tmp);
        write(record.getValue(), channel, tmp);
    }

    private void write(ByteBuffer val, WritableByteChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        tmp.putInt(val.remaining());
        tmp.position(0);
        channel.write(tmp);
        channel.write(val);
    }

    private Record readRecord(ReadableByteChannel channel, ByteBuffer tmp) throws IOException {
        return Record.of(readVal(channel, tmp), readVal(channel, tmp));
    }

    private ByteBuffer readVal(ReadableByteChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        channel.read(tmp);
        tmp.position(0);
        final ByteBuffer byteBuffer = ByteBuffer.allocate(tmp.getInt());
        channel.read(byteBuffer);
        byteBuffer.position(0);
        return byteBuffer;
    }

}
