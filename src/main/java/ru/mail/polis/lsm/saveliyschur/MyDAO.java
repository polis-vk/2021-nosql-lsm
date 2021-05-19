package ru.mail.polis.lsm.saveliyschur;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MyDAO implements DAO {

    private volatile ConcurrentSkipListMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    private final DAOConfig config;
    private static final String FILE_SAVE = "save.dat";
    private static final String LOG_FILE = "log_db.log";
    private static Path fileLog;

    public MyDAO(DAOConfig config) throws IOException {
        this.config = config;
        fileLog = config.getDir().resolve(LOG_FILE);

        Path file = config.getDir().resolve(FILE_SAVE);

        if (Files.exists(file)) {
            readFileAndAddCollection(file);
        }

        //Create a log file if it does not exist
        //Read data from the log file, if any
        File logFileCreate = new File(String.valueOf(fileLog.toFile()));
        if (!logFileCreate.exists()){
            logFileCreate.createNewFile();
        } else {
            readFileAndAddCollection(fileLog);
        }

    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        ConcurrentNavigableMap<ByteBuffer, Record> storageHash = storage.clone();
        return getSubMap(fromKey, toKey, storageHash).values().stream().filter(x -> x.getValue() != null).iterator();
    }

    @Override
    public void upsert(Record record) {
        storage.put(record.getKey(), record);

        //We write the entry to the log so that in case of a failure we can recover
        try (FileChannel fileChannel = FileChannel.open(fileLog, StandardOpenOption.WRITE)){
            ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            writeInt(record.getKey(), fileChannel, size);
            writeInt(record.getValue(), fileChannel, size);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        Files.deleteIfExists(config.getDir().resolve(FILE_SAVE));

        Path file = config.getDir().resolve(FILE_SAVE);

        try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            for (Record record : storage.values()) {
                writeInt(record.getKey(), fileChannel, size);
                writeInt(record.getValue(), fileChannel, size);
            }
        }
        File logFileCreate = new File(String.valueOf(fileLog.toFile()));
        logFileCreate.delete();
    }

    public DAOConfig getConfig() {
        return config;
    }

    private ConcurrentNavigableMap<ByteBuffer, Record> getSubMap(@Nullable ByteBuffer fromKey,
                                                                 @Nullable ByteBuffer toKey,
                                                                 ConcurrentNavigableMap<ByteBuffer, Record> map) {
        if (fromKey == null && toKey == null)
            return map;
        else if (fromKey == null)
            return map.headMap(toKey);
        else if (toKey == null)
            return map.tailMap(fromKey);
        else
            return map.subMap(fromKey, toKey);
    }

    /**
     *
     * @param file
     * @throws IOException
     */
    private void readFileAndAddCollection(Path file) throws IOException {
        final ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
        try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ)) {
            while (fileChannel.position() < fileChannel.size()) {
                ByteBuffer key = readValue(fileChannel, size);
                ByteBuffer value = readValue(fileChannel, size);
                if (value == null){
                    storage.put(key, Record.tombstone(key));
                    continue;
                }
                storage.put(key, Record.of(key, value));
            }
        }
    }

    private static ByteBuffer readValue(ReadableByteChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        channel.read(tmp);
        tmp.position(0);
        int num = tmp.getInt();
        if(num == -1){
            return null;
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(num);
        channel.read(byteBuffer);
        byteBuffer.position(0);
        return byteBuffer;
    }

    private static void writeInt(ByteBuffer value, WritableByteChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        if(value == null){
            tmp.putInt(-1);
            tmp.position(0);
            channel.write(tmp);
        } else {
            tmp.putInt(value.remaining());
            tmp.position(0);
            channel.write(tmp);
            channel.write(value);
        }
    }
}
