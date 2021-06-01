package ru.mail.polis.lsm.gromovmaxim;

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
            var classS = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = classS.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();

    private final Path saveFileName;
    private final Path tmpFileName;

    private final MappedByteBuffer mmap;

    /**
     * Constructor that initialize buffers.
     */

    public NotJustInMemoryDAO(DAOConfig config) throws IOException {
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
        try (FileChannel fileChannel = FileChannel.open(
                tmpFileName,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {
            ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            for (Record record : storage.values()) {
                writeInt(record.getKey(), fileChannel, size);
                writeInt(record.getValue(), fileChannel, size);
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

    private static void writeInt(ByteBuffer value, WritableByteChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        tmp.putInt(value.remaining());
        tmp.position(0);
        channel.write(tmp);
        channel.write(value);
    }

    private SortedMap<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
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

}
