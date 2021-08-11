package ru.mail.polis.lsm.saveliyschur.sstservice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.saveliyschur.utils.PeekingIterator;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

import static ru.mail.polis.lsm.saveliyschur.utils.UtilsIterator.*;

public class LSMDao implements DAO {

    private static final Logger log = LoggerFactory.getLogger(LSMDao.class);

    private final SortedMap<ByteBuffer, Record> memoryStorage = new ConcurrentSkipListMap<>();
    private final ConcurrentLinkedDeque<ISSTable> tables = new ConcurrentLinkedDeque<>();

    private final Thread compact;

    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final DAOConfig config;

    @GuardedBy("this")
    private int nextSSTableIndex;
    @GuardedBy("this")
    private int memoryConsumption;



    public LSMDao(DAOConfig config) throws IOException {
        this.config = config;
        List<ISSTable> ssTables = Loader.loadFromDir(config.getDir());
        nextSSTableIndex = ssTables.size();
        tables.addAll(ssTables);
        compact = startCompactThread();
        compact.start();
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        synchronized (this) {
            log.info("Get range");
            Iterator<Record> sstableRanges = sstableRanges(fromKey, toKey);
            Iterator<Record> memoryRange = map(fromKey, toKey).values().iterator();
            Iterator<Record> iterator = mergeTwo(new PeekingIterator(sstableRanges), new PeekingIterator(memoryRange));
            return filterTombstones(iterator);
        }
    }

    @Override
    public void upsert(Record record) {
        synchronized (this) {
            log.info("Upset record");
            memoryConsumption += record.getSize();
            if (memoryConsumption > config.memoryLimit) {
                try {
                    log.info("Flush memory");
                    flush();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                memoryConsumption = record.getSize();
            }
        }

        memoryStorage.put(record.getKey(), record);
    }

    private Thread startCompactThread() {
        return new Thread(() -> {
            try {
                while (true) {
                    fonCompact();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private void fonCompact() throws IOException {
        if (tables.size() > 1) {
            synchronized (this) {
                if (tables.size() > 1) {
                    log.info("Compact");

                    Path dir = config.getDir();

                    Stream<Path> files = Files.list(dir);
                    long countTmp = files.filter(f -> f.toString().contains("tmp")).count();

                    Path file = dir.resolve("file_compact_" + countTmp);

                    Iterator<Record> sstableRanges = filterTombstones(sstableRanges(null, null));
                    ISSTable issTable = SSTableFileWorker.write(sstableRanges, file);

                    tables.forEach(ISSTable::deleteWithClose);
                    tables.clear();
                    issTable.close();

                    File compactFile = new File(file.toString());
                    File sstableFileCompact = new File(dir.resolve("file_0").toString());

                    boolean succsessRename = compactFile.renameTo(sstableFileCompact);
                    if (!succsessRename) {
                        throw new FileSystemException("Error rename file before compact");
                    }
                    ISSTable compactSSTable = new SSTable(compactFile.toPath());
                    tables.add(compactSSTable);
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (!memoryStorage.isEmpty()) {
                flush();
            }
            sstableClose();
            compact.interrupt();
        }
    }

    private void sstableClose() throws IOException {
        for (ISSTable sstable : tables) {
            sstable.close();
        }
    }

    @GuardedBy("this")
    private void flush() throws IOException {
        Path dir = config.getDir();
        Path file = dir.resolve("file_" + nextSSTableIndex);
        nextSSTableIndex++;

        ISSTable ssTable = SSTableFileWorker.write(memoryStorage.values().iterator(), file);
        tables.add(ssTable);
        memoryStorage.clear();
    }

    private Iterator<Record> sstableRanges(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> iterators = new ArrayList<>(tables.size());
        for (ISSTable ssTable : tables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return merge(iterators);
    }

    private SortedMap<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return memoryStorage;
        }
        if (fromKey == null) {
            return memoryStorage.headMap(toKey);
        }
        if (toKey == null) {
            return memoryStorage.tailMap(fromKey);
        }
        return memoryStorage.subMap(fromKey, toKey);
    }
}
