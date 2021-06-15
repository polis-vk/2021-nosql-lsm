package ru.mail.polis.lsm.segu.service;

import ru.mail.polis.lsm.DAO;
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
import java.util.*;
import java.util.stream.Collectors;

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

    /**
     * Write SS Table to disk.
     *
     * @param ssTable - ss table object
     * @param storage - current storage
     * @throws IOException - it throws IO exception
     */
    public void writeTable(SSTable ssTable, Collection<Record> storage) throws IOException {
        int index = 0;
        int currentOffset = 0;
        try (FileChannel fileChannel = FileChannel.open(ssTable.getFilePath(),
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE)) {

            for (final Record record : storage) {
                ByteBuffer recordKey = record.getKey();
                ByteBuffer recordValue = record.getValue();

                writeValueToTableFile(fileChannel, recordKey);
                writeValueToTableFile(fileChannel, recordValue);

                //IndexRecord indexRecord = new IndexRecord(index, currentOffset);
                //writeValueToIndexFile(indexFileChannel, indexRecord); // Currently not implemented

                index++;
                currentOffset += record.size();
            }
        }
    }

    /**
     * Get range.
     *
     * @param ssTablesDeque         - ss tables
     * @param rangedStorageIterator - storage iterator
     * @param from                  - from key
     * @param to                    - to key
     * @return merged iterator in range
     */
    public Iterator<Record> getRange(Deque<SSTable> ssTablesDeque,
                                     Iterator<Record> rangedStorageIterator, ByteBuffer from, ByteBuffer to) {
        List<Iterator<Record>> rangeIterators = ssTablesDeque.stream()
                .map(ssTable -> getRange(ssTable, from, to))
                .collect(Collectors.toList());
        rangeIterators.add(rangedStorageIterator);
        return DAO.merge(rangeIterators);
    }

    private Iterator<Record> getRange(SSTable ssTable, ByteBuffer from, ByteBuffer to) {
        SortedMap<ByteBuffer, Record> resultMap = new TreeMap<>();
        try (FileChannel fileChannel = FileChannel.open(ssTable.getFilePath(), StandardOpenOption.READ)) {
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());

            while (mappedByteBuffer.hasRemaining()) {
                int keySize = mappedByteBuffer.getInt();
                ByteBuffer key = mappedByteBuffer.slice().limit(keySize).asReadOnlyBuffer();

                mappedByteBuffer.position(mappedByteBuffer.position() + keySize);

                int valueSize = mappedByteBuffer.getInt();

                if (!isInRange(key, from, to)) {
                    if (valueSize >= 0) {
                        mappedByteBuffer.position(mappedByteBuffer.position() + valueSize);
                    }
                    continue;
                }

                if (valueSize >= 0) {
                    ByteBuffer value = mappedByteBuffer.slice().limit(valueSize).asReadOnlyBuffer();
                    resultMap.put(key, Record.of(key, value));
                } else {
                    continue;
                }
                if (mappedByteBuffer.hasRemaining()) {
                    mappedByteBuffer.position(mappedByteBuffer.position() + valueSize);
                }
            }
            cleanMappedByteBuffer(mappedByteBuffer);
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Failed getting range");
        }
        return resultMap.values().iterator();
    }

    private boolean isInRange(ByteBuffer key, ByteBuffer from, ByteBuffer to) {
        if (from == null && to == null) {
            return true;
        } else if (to == null) {
            return key.compareTo(from) >= 0;
        } else if (from == null) {
            return key.compareTo(to) < 0;
        }
        return key.compareTo(from) >= 0 && key.compareTo(to) < 0;
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

    /**
     * Clean MappedByteBuffer.
     *
     * @param mappedByteBuffer - byte buffer
     */
    public void cleanMappedByteBuffer(@Nullable MappedByteBuffer mappedByteBuffer) {
        if (mappedByteBuffer != null) {
            try {
                CLEAN.invoke(null, mappedByteBuffer);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    /**
     * Resolve path for DAO.
     *
     * @param config      - config
     * @param index       - index
     * @param filePrefix  - file prefix
     * @param indexPrefix - index prefix
     * @return - path
     */
    public SSTablePath resolvePath(DAOConfig config, int index, String filePrefix, String indexPrefix) {
        String fileName = filePrefix + index;
        String fileIndexName = indexPrefix + index;
        Path fileNamePath = config.getDir().resolve(fileName);
        Path indexFileNamePath = config.getDir().resolve(fileIndexName);
        return new SSTablePath(fileNamePath, indexFileNamePath);
    }
}
