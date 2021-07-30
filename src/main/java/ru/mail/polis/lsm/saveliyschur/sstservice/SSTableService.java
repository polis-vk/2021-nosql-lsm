package ru.mail.polis.lsm.saveliyschur.sstservice;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.saveliyschur.utils.UtilsIterator;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class SSTableService {

    private static final Logger log = Logger.getLogger(SSTableService.class.getName());

    public SSTableService(DAOConfig config) {
    }

    public Iterator<Record> getRange(Deque<SSTable> ssTables, ByteBuffer from, ByteBuffer to) {
        log.info("Get range.");
        return UtilsIterator.merge(ssTables.stream()
                .map(ssTable -> readSSTable(ssTable, from, to))
                .collect(Collectors.toList()));
    }

    public SortedMap<ByteBuffer, Record> readSSTable(SSTable ssTable) {
        Path file = ssTable.getPath();
        log.info("Read SSTable from path: " + file.toString());

        if (ssTable.getResultMap() != null) {
            return ssTable.getResultMap();
        }

        SortedMap<ByteBuffer, Record> resultMap = new TreeMap<>();
        try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ)) {
            MappedByteBuffer mappedByteBuffer;
                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());

            while (mappedByteBuffer.hasRemaining()) {
                int keySize = mappedByteBuffer.getInt();
                ByteBuffer key = mappedByteBuffer.slice().limit(keySize).asReadOnlyBuffer();

                mappedByteBuffer.position(mappedByteBuffer.position() + keySize);

                int valueSize = mappedByteBuffer.getInt();

                if (valueSize >= 0) {
                    ByteBuffer value = mappedByteBuffer.slice().limit(valueSize).asReadOnlyBuffer();
                    resultMap.put(key, Record.of(key, value));
                } else {
                    resultMap.put(key, Record.tombstone(key));
                }
                if (mappedByteBuffer.hasRemaining()) {
                    mappedByteBuffer.position(mappedByteBuffer.position() + valueSize);
                }
            }
            ssTable.setMapp(mappedByteBuffer);
        } catch (IOException e) {
            e.printStackTrace();
            log.severe("Error in range!");
        }
        ssTable.setResultMap(resultMap);

        return resultMap;
    }

    private Iterator<Record> readSSTable(SSTable ssTable, ByteBuffer from, ByteBuffer to) {
        return getSubMap(from, to, readSSTable(ssTable)).values().iterator();
    }

    private SortedMap<ByteBuffer, Record> getSubMap(@Nullable ByteBuffer fromKey,
                                                                 @Nullable ByteBuffer toKey,
                                                    SortedMap<ByteBuffer, Record> map) {
        if (fromKey == null && toKey == null) {
            return map;
        }
        else if (fromKey == null) {
            return map.headMap(toKey);
        }
        else if (toKey == null) {
            return map.tailMap(fromKey);
        }
        else {
            return map.subMap(fromKey, toKey);
        }
    }

    public void flush(AbstractMap<ByteBuffer, Record> storage, SSTable ssTable) throws IOException {
        Path file = ssTable.getPath();
        log.info("Write SSTable to path: " + file.toString());
        try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            for (Record record : storage.values()) {
                writeInt(record.getKey(), fileChannel, size);
                writeInt(record.getValue(), fileChannel, size);
            }
        }
    }

    private static void writeInt(ByteBuffer value, WritableByteChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        if(value == null) {
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
