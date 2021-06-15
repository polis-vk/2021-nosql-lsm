package ru.mail.polis.lsm.segu.service;

import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.segu.model.IndexRecord;
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
import java.util.Collections;
import java.util.Iterator;
import java.util.TreeMap;

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

    private final ByteBuffer defaultIntBuffer = ByteBuffer.allocate(Integer.BYTES);


    public Iterator<Record> loadTableFile(SSTable ssTable) throws IOException {
        TreeMap<ByteBuffer, Record> resultMap = new TreeMap<>();
        try (FileChannel fileChannel = FileChannel.open(ssTable.getFilePath(), StandardOpenOption.READ)) {
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
            while (mappedByteBuffer.hasRemaining()) {
                int keySize = mappedByteBuffer.getInt();
                ByteBuffer key = mappedByteBuffer.slice().limit(keySize).asReadOnlyBuffer();

                mappedByteBuffer.position(mappedByteBuffer.position() + keySize);

                int valueSize = mappedByteBuffer.getInt();

                if (valueSize < 0) {
                    resultMap.put(key, Record.tombstone(key));
                } else {
                    ByteBuffer value = mappedByteBuffer.slice().limit(valueSize).asReadOnlyBuffer();
                    resultMap.put(key, Record.of(key, value));
                }
                if (mappedByteBuffer.hasRemaining()) {
                    mappedByteBuffer.position(mappedByteBuffer.position() + valueSize);
                }
            }
        }
        cleanMappedByteBuffer(mappedByteBuffer);
        return Collections.emptyIterator();
    }

    public void writeTableAndIndexFile(SSTable ssTable) throws IOException {
        int index = 0;
        int currentOffset = 0;
        try (FileChannel fileChannel = FileChannel.open(ssTable.getFilePath(),
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);
             FileChannel indexFileChannel = FileChannel.open(ssTable.getIndexFilePath(),
                     StandardOpenOption.TRUNCATE_EXISTING,
                     StandardOpenOption.CREATE,
                     StandardOpenOption.WRITE)) {

            for (final Record record : ssTable.getStorage().values()) {
                ByteBuffer recordKey = record.getKey();
                ByteBuffer recordValue = record.getValue();

                writeValueToTableFile(fileChannel, recordKey);
                writeValueToTableFile(fileChannel, recordValue);

                IndexRecord indexRecord = new IndexRecord(index, currentOffset);
                writeValueToIndexFile(indexFileChannel, indexRecord);

                index++;
                currentOffset += record.size();
            }
        }


    }

    private void writeValueToTableFile(FileChannel fileChannel, @Nullable ByteBuffer valueBuffer) throws IOException {
        if (valueBuffer == null) {
            writeSize(fileChannel, defaultIntBuffer, -1);
        } else {
            writeSize(fileChannel, defaultIntBuffer, valueBuffer.remaining());
            fileChannel.write(valueBuffer);
        }
    }

    private void writeSize(FileChannel fileChannel, ByteBuffer sizeBuffer, int size) throws IOException {
        sizeBuffer.position(0);
        sizeBuffer.putInt(size);
        sizeBuffer.position(0);
        fileChannel.write(sizeBuffer);
    }

    private void writeValueToIndexFile(FileChannel fileChannel, IndexRecord indexRecord) throws IOException {
        defaultIntBuffer.position(0);

        defaultIntBuffer.putInt(indexRecord.getIndex());
        defaultIntBuffer.position(0);
        fileChannel.write(defaultIntBuffer);

        defaultIntBuffer.position(0);

        defaultIntBuffer.putInt(indexRecord.getOffset());
        defaultIntBuffer.position(0);
        fileChannel.write(defaultIntBuffer);
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
