package ru.mail.polis.lsm.veronika_peutina;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
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
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class NotJustInMemoryDAO implements DAO {
    private static final Method CLEAN;
    static {
        try {
            Class<?> aClass = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = aClass.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    private final DAOConfig config;
    private final Path saveFileName;
    private final Path tmpFileName;

    private final MappedByteBuffer mmap;

    public NotJustInMemoryDAO(DAOConfig config) throws IOException {
        this.config = config;
        Path dir = config.getDir();
        saveFileName = dir.resolve("save.dat");
        tmpFileName = dir.resolve("tmp.dat");
        if (!Files.exists(saveFileName)) {
            if (Files.exists(tmpFileName)) {
                Files.move(tmpFileName, saveFileName, StandardCopyOption.ATOMIC_MOVE);
            } else {
                mmap = null;
                return;
            }
        }

        try (FileChannel channel = FileChannel.open(saveFileName, StandardOpenOption.READ)) {
            mmap = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
            while (mmap.hasRemaining()) {
                int keySize = mmap.getInt();
                ByteBuffer key = mmap.slice().limit(keySize).asReadOnlyBuffer();

                mmap.position(mmap.position() + keySize);

                int valueSize = mmap.getInt();
                ByteBuffer value = mmap.slice().limit(valueSize).asReadOnlyBuffer();

                mmap.position(mmap.position() + valueSize);

                storage.put(key, Record.of(key, value));
            }
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
        try (FileChannel fileChannel = FileChannel.open(
                tmpFileName,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {
            ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            for (Record record : storage.values()) {
                writeRecord(record  , fileChannel, size);
            }
            fileChannel.force(false);
        }

        if (mmap != null) {
            try {
                CLEAN.invoke(null, mmap);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IOException(e);
            }
        }

        Files.deleteIfExists(saveFileName);
        Files.move(tmpFileName, saveFileName, StandardCopyOption.ATOMIC_MOVE);
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


}
