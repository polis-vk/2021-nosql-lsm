package ru.mail.polis.lsm.segu.sstable;

import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class SSTableService {

    private MappedByteBuffer mappedByteBuffer;
    private final Method CLEAN;

    {
        Class<?> clazz;
        try {
            clazz = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = clazz.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (NoSuchMethodException | ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    public void load(SSTable ssTable, Path path) {

        cleanMappedByteBuffer(mappedByteBuffer);
    }

    public void write(SSTable ssTable) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(ssTable.getFilePath(),
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE)) {
            final ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            for (final Record record : ssTable.getStorage().values()) {
                writeValue(fileChannel, record.getKey(), size);
                writeValue(fileChannel, record.getValue(), size);
            }
        }
    }

    private void writeValue(FileChannel fileChannel, @Nullable ByteBuffer value, ByteBuffer size) throws IOException {
        if (value == null) {
            writeSize(fileChannel, size, -1);
        } else {
            writeSize(fileChannel, size, value.remaining());
            fileChannel.write(value);
        }
    }

    private void writeSize(FileChannel fileChannel, ByteBuffer sizeBuffer, int size) throws IOException {
        sizeBuffer.position(0);
        sizeBuffer.putInt(size);
        sizeBuffer.position(0);
        fileChannel.write(sizeBuffer);
    }

    public void cleanMappedByteBuffer(@Nullable MappedByteBuffer mappedByteBuffer) {
        if (mappedByteBuffer != null) {
            try {
                CLEAN.invoke(null, mappedByteBuffer);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public SSTablePath resolvePath(DAOConfig config, int index, String filePrefix, String indexPrefix) {
        String fileName = filePrefix + index;
        String fileIndexName = indexPrefix + index;
        Path fileNamePath = config.getDir().resolve(fileName);
        Path indexFileNamePath = config.getDir().resolve(fileIndexName);
        return new SSTablePath(fileNamePath, indexFileNamePath);
    }
}
