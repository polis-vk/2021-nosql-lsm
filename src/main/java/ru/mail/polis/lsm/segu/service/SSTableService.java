package ru.mail.polis.lsm.segu.service;

import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.segu.model.SSTable;
import ru.mail.polis.lsm.segu.model.SSTablePath;

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
    private static final Method CLEAN;

    static {
        Class<?> clazz;
        try {
            clazz = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = clazz.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (NoSuchMethodException | ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    public void loadTableFile(SSTable ssTable, Path path) {

        cleanMappedByteBuffer(mappedByteBuffer);
    }

    public void writeTableAndIndexFile(SSTable ssTable) throws IOException {
        int index = 0;
        int currentOffset = 0;
        try (FileChannel fileChannel = FileChannel.open(ssTable.getFilePath(),
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE)) {
            final ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            for (final Record record : ssTable.getStorage().values()) {
                writeValueToTableFile(fileChannel, record.getKey(), size);
                writeValueToTableFile(fileChannel, record.getValue(), size);
                writeValueToIndexFile(index, record.getKey(), currentOffset); // TODO
                index++;
                currentOffset += record.size();
            }
        }
    }

    private void writeValueToTableFile(FileChannel fileChannel, @Nullable ByteBuffer value, ByteBuffer size) throws IOException {
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

    private void writeValueToIndexFile(int index, ByteBuffer key, Integer offset) {

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
