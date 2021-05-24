package ru.mail.polis.lsm;

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
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DaoImpl implements DAO {

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

    private final DAOConfig config;
    private final NavigableMap<ByteBuffer, Record> map;
    private final Path saveFileName;
    private final Path tmpFileName;

    private final MappedByteBuffer mmap;

    /**
     * Implementation of DAO with Persistence.
     */
    public DaoImpl(DAOConfig config) throws IOException {
        this.config = config;
        this.map = new ConcurrentSkipListMap<>();

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
        try (FileChannel fileChannel = FileChannel.open(saveFileName, StandardOpenOption.READ)) {
            mmap = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
            while (mmap.hasRemaining()) {
                int keySize = mmap.getInt();
                ByteBuffer key = mmap.slice().limit(keySize).asReadOnlyBuffer();

                mmap.position(mmap.position() + keySize);

                int valueSize = mmap.getInt();
                ByteBuffer value = mmap.slice().limit(valueSize).asReadOnlyBuffer();

                mmap.position(mmap.position() + valueSize);

                map.put(key, Record.of(key, value));
            }
        }
    }

    private ByteBuffer readBuffer(FileChannel fileChannel, ByteBuffer buffer) throws IOException {
        buffer.position(0);
        fileChannel.read(buffer);
        buffer.position(0);
        final ByteBuffer tmp = ByteBuffer.allocate(buffer.getInt());
        fileChannel.read(tmp);
        tmp.position(0);
        return tmp;
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return map(fromKey, toKey).values().stream()
                .filter(record -> record.getValue() != null)
                .iterator();
    }

    private SortedMap<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey,@Nullable ByteBuffer toKey) {
        if ((fromKey == null) && (toKey == null)) {
            return map;
        }

        if (fromKey == null) {
            return map.headMap(toKey);
        }

        if (toKey == null) {
            return map.tailMap(fromKey);
        }

        return map.subMap(fromKey, toKey);
    }

    @Override
    public void upsert(Record record) {
        map.put(record.getKey(), record);
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
            for (Record record : map.values()) {
                if (!record.isTombstone()) {
                    writeInt(record.getKey(), fileChannel, size);
                    writeInt(record.getValue(), fileChannel, size);
                }
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

}
