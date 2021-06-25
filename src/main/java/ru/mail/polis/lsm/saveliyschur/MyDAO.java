package ru.mail.polis.lsm.saveliyschur;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.saveliyschur.sstservice.SSTable;
import ru.mail.polis.lsm.saveliyschur.sstservice.SSTableService;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class MyDAO implements DAO {

    private static final Logger log = Logger.getLogger(MyDAO.class.getName());

    private final ConcurrentSkipListMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    
    private Long sizeCollections = 0L;
    private static final Long maxSizeCollection = 2 * 1024 * 1024L;

    private final DAOConfig config;
    private static final String LOG_FILE = "log_db.log";
    private static Path fileLog;

    private long numberSSTables;

    private List<SSTable> ssTables = Collections.synchronizedList(new ArrayList<>());

    private final SSTableService ssTableService;

    public MyDAO(DAOConfig config) throws IOException {
        log.info("Create DAO from path: " + config.getDir().toString());
        this.config = config;
        fileLog = config.getDir().resolve(LOG_FILE);
        ssTableService = new SSTableService(config);

        try (Stream<Path> streamPath = Files.walk(config.getDir())) {
            log.info("Read SSTables");
            streamPath.filter(file -> file.toString().endsWith(SSTable.EXTENSION))
                    .map(SSTable::new)
                    .forEach(ssTables::add);

            numberSSTables = ssTables.size();
            log.info("SSTables number = " + numberSSTables);
        }

        //Create a log file if it does not exist
        //Read data from the log file, if any
        File logFileCreate = new File(String.valueOf(fileLog.toFile()));
        if (!logFileCreate.exists()) {
            logFileCreate.createNewFile();
        } else {
            readFileAndAddCollection(fileLog);
        }

    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
            ConcurrentNavigableMap<ByteBuffer, Record> storageHash = storage.clone();
            Iterator<Record> memoryIterator = getSubMap(fromKey, toKey, storageHash).values().stream()
                    .iterator();
            Deque<SSTable> ssTablesDeque = new ConcurrentLinkedDeque<>();
            ssTables.stream().sorted(SSTable::compareTo).forEach(ssTablesDeque::add);

            Iterator<Record> sstableIterators = ssTableService.getRange(ssTablesDeque, fromKey, toKey);
            Iterator<Record> anser = DAO.mergeTwo(new PeekingIterator(sstableIterators), new PeekingIterator(memoryIterator));
            return filterTombstones(anser);
    }

    @Override
    public void upsert(Record record) {
        synchronized (this) {
            //Проверяем содержит ли коллекция элемент, чтобы заменить размеры
            if (storage.containsKey(record.getKey())) {
                Record value = storage.get(record.getKey());
                sizeCollections = sizeCollections - value.getSize();
            }

            storage.put(record.getKey(), record);

            sizeCollections = sizeCollections + record.getSize();

            //We write the entry to the log so that in case of a failure we can recover
            try (FileChannel fileChannel = FileChannel.open(fileLog, StandardOpenOption.WRITE)) {
                ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
                writeInt(record.getKey(), fileChannel, size);
                writeInt(record.getValue(), fileChannel, size);
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (sizeCollections >= maxSizeCollection) {
                log.info("Flush file in upset");
                try {
                    ssTableService.flush(storage, create());
                    storage.clear();
                    Files.deleteIfExists(fileLog);
                    File logFileCreate = new File(String.valueOf(fileLog.toFile()));
                    logFileCreate.createNewFile();
                    sizeCollections = 0L;
                } catch (IOException e) {
                    e.printStackTrace();
                    log.severe("Error write file in upset");
                }
                log.info("OK flush file in upset");
            }
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (!storage.isEmpty()) {
                ssTableService.flush(storage, create());
            }
            storage.clear();
            ssTables.forEach(s -> {
                try {
                    s.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            ssTables.clear();
            Files.deleteIfExists(fileLog);
            log.info("Close ok!");
        }
    }

    @Override
    public void compact() {
        synchronized (this) {
            List<SSTable> ssTablesWithSuffics = Collections.synchronizedList(new ArrayList<>());
            for (int i = 0; i < ssTables.size(); i = i + 2) {
                try {
                    SSTable ssTable = compactTwoFile(ssTables.get(i), ssTables.get(i+1), ssTablesWithSuffics.size());
                    ssTablesWithSuffics.add(ssTable);
                } catch (IOException e) {
                    e.printStackTrace();
                    log.severe("Ошибка при слиянии compact");
                }
            }

            ssTables.forEach(s -> {
                try {
                    s.close();
                    Files.deleteIfExists(s.getPath());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            ssTablesWithSuffics.forEach(ssTable -> {
                File file = new File(ssTable.getPath().toString());
                File newFile = new File(ssTable.getPath().toString().replaceAll(SSTable.SUFFICS, ""));

                boolean flag = file.renameTo(newFile);
                if (!flag) {
                    log.severe("Don't rename file for path " + ssTable.getPath().toString());
                }

                ssTable.setPath(newFile.toPath());
            });
            ssTables = ssTablesWithSuffics;
        }
    }

    public SSTable compactTwoFile(SSTable first, SSTable second, long i) throws IOException {
        SortedMap<ByteBuffer, Record> firstMap = ssTableService.readSSTable(first);
        SortedMap<ByteBuffer, Record> secondMap = ssTableService.readSSTable(second);

        SSTable ssTable = createWithSuffics(i);
        Iterator<Record> iterator = DAO.mergeTwo(new PeekingIterator(firstMap.values().iterator()),
                new PeekingIterator(secondMap.values().iterator()));
        AbstractMap<ByteBuffer, Record> flusherMap = new TreeMap<>();
        while (iterator.hasNext()) {
            Record record = iterator.next();
            flusherMap.put(record.getKey(), record);
        }
        ssTableService.flush(flusherMap, ssTable);
        return ssTable;
    }

    private ConcurrentNavigableMap<ByteBuffer, Record> getSubMap(@Nullable ByteBuffer fromKey,
                                                                 @Nullable ByteBuffer toKey,
                                                                 ConcurrentNavigableMap<ByteBuffer, Record> map) {
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

    /**
     *Read file and add key-value in collection
     * @param file file for read
     * @throws IOException file problem
     */
    private void readFileAndAddCollection(Path file) throws IOException {
        final ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
        try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ)) {
            while (fileChannel.position() < fileChannel.size()) {
                ByteBuffer key = readValue(fileChannel, size);
                ByteBuffer value = readValue(fileChannel, size);
                if (value == null) {
                    storage.put(key, Record.tombstone(key));
                    continue;
                }
                storage.put(key, Record.of(key, value));
            }
        }
    }

    private static ByteBuffer readValue(ReadableByteChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        channel.read(tmp);
        tmp.position(0);
        int num = tmp.getInt();
        if(num == -1) {
            return null;
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(num);
        channel.read(byteBuffer);
        byteBuffer.position(0);
        return byteBuffer;
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

    private SSTable create(){
        SSTable ssTable = new SSTable(config.getDir().resolve(SSTable.NAME
                + numberSSTables
                + SSTable.EXTENSION));
        numberSSTables ++;
        ssTables.add(ssTable);
        return ssTable;
    }

    private SSTable createWithSuffics(long i) {
        return new SSTable(config.getDir().resolve(SSTable.NAME
                + i
                + SSTable.EXTENSION + SSTable.SUFFICS));
    }

    private static Iterator<Record> filterTombstones(Iterator<Record> iterator) {
        PeekingIterator delegate = new PeekingIterator(iterator);
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                for (;;) {
                    Record peek = delegate.peek();
                    if (peek == null) {
                        return false;
                    }
                    if (!peek.isTombstone()) {
                        return true;
                    }

                    delegate.next();
                }
            }

            @Override
            public Record next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("No elements");
                }
                return delegate.next();
            }
        };
    }
}
