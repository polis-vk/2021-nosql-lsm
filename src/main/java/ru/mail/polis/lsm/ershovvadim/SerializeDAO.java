package ru.mail.polis.lsm.ershovvadim;

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

/**
 * implementation Serialize DAO.
 */
public class SerializeDAO implements DAO {
    private static final Method CLEAN;
    private static final String FILE_TO_SAVE = "DAO_FILE_TO_SAVE.dat";
    private static final String TEMP_FILE_TO_SAVE = "DAO_TEMP_FILE_TO_SAVE.dat";

    static {
        try {
            Class<?> fileChannelImplClass = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = fileChannelImplClass.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    private final DAOConfig config;
    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    private final ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);

    private Path saveFileName;
    private Path tempFileName;

    private MappedByteBuffer mmap;

    /**
     * Constructor SerializeDAO object, deserialize data from file.
     *
     * @param config DAOConfig
     */
    public SerializeDAO(DAOConfig config) throws IOException {
        this.config = config;
        getBackStorage();
    }

    private void getBackStorage() throws IOException {
        saveFileName = this.config.getDir().resolve(FILE_TO_SAVE);
        tempFileName = this.config.getDir().resolve(TEMP_FILE_TO_SAVE);

        if (!Files.exists(saveFileName)) {
            if (Files.exists(tempFileName)) {
                Files.move(tempFileName, saveFileName, StandardCopyOption.ATOMIC_MOVE);
            } else {
                mmap = null;
                return;
            }
        }

        try (FileChannel fileChannel = FileChannel.open(saveFileName, StandardOpenOption.READ)) {
            mmap = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
            while (mmap.hasRemaining()) {
                int keySize = mmap.getInt();
                ByteBuffer key = mmap.slice().limit(keySize).slice();

                mmap.position(mmap.position() + keySize);

                int valueSize = mmap.getInt();
                ByteBuffer value = mmap.slice().limit(valueSize).slice();

                mmap.position(mmap.position() + valueSize);

                storage.put(key, Record.of(key, value));
            }
        }
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
        if (recordUpsert.isTombstone()) {
            storage.remove(recordUpsert.getKey());
        } else {
            storage.put(recordUpsert.getKey(), recordUpsert);
        }
    }

    @Override
    public void close() throws IOException {
        try (FileChannel fileChannel = FileChannel.open(
                tempFileName,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {
            for (Record recordFor : storage.values()) {
                write(recordFor.getKey(), fileChannel);
                write(recordFor.getValue(), fileChannel);
            }
            fileChannel.force(false);
        }

        if (mmap != null) {
            try {
                CLEAN.invoke(null, mmap);
            } catch (IllegalAccessError | InvocationTargetException | IllegalAccessException e) {
                throw new IOException(e);
            }
        }

        Files.deleteIfExists(saveFileName);
        Files.move(tempFileName, saveFileName, StandardCopyOption.ATOMIC_MOVE);
    }

    private void write(ByteBuffer value, WritableByteChannel fileChannel) throws IOException {
        size.position(0);
        size.putInt(value.remaining());
        size.position(0);
        fileChannel.write(size);
        fileChannel.write(value);
    }
}
