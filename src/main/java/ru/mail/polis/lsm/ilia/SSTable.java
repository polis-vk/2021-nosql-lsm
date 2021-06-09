package ru.mail.polis.lsm.ilia;

import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;

class SSTable {

    private static final Method CLEAN;

    static {
        try {
            Class<?> filename = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = filename.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    private final MappedByteBuffer mmap;

    static List<SSTable> loadFromDir(Path dir) {

    }

    static SSTable write(Iterator<Record> records, Path file) {

        try (FileChannel fileChannel = FileChannel.open(
                tmpFileName,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE_NEW
        )) {
            ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            for (Record record : memoryStorage.values()) {
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

    SStable(Path file) {
        try (FileChannel channel = FileChannel.open(saveFileName, StandardOpenOption.READ)) {
            mmap = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
            while (mmap.hasRemaining()) {
                int keySize = mmap.getInt();
                ByteBuffer key = mmap.slice().limit(keySize).asReadOnlyBuffer();
                mmap.position(mmap.position() + keySize);

                int valueSize = mmap.getInt();
                ByteBuffer value = mmap.slice().limit(valueSize).asReadOnlyBuffer();
                mmap.position(mmap.position() + valueSize);

                memoryStorage.put(key, Record.of(key, value));
            }
        }
    }

    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {

    }
}
