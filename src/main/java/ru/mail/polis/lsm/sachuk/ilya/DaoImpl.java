package ru.mail.polis.lsm.sachuk.ilya;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.sachuk.ilya.iterators.MergeIterator;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class DaoImpl implements DAO {

    private static final long LIMIT = 16L * 1024 * 1024;

    private final DAOConfig config;
    private final SortedMap<ByteBuffer, Record> memoryStorage = new ConcurrentSkipListMap<>();
    private final List<SSTable> ssTables = new ArrayList<>();

    private long memoryConsumption;
    private int nextSSTableNumber;

    /**
     * Constructor that initialize path and restore storage.
     *
     * @param config is config.
     * @throws IOException is thrown when an I/O error occurs.
     */
    public DaoImpl(DAOConfig config) throws IOException {
        this.config = config;

        ssTables.addAll(SSTable.loadFromDir(config.getDir()));
        nextSSTableNumber = ssTables.size();
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey)
            throws UncheckedIOException {
        synchronized (this) {

            Iterator<Record> ssTableRanges;
            try {
                ssTableRanges = ssTableRanges(fromKey, toKey);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            Iterator<Record> memoryRange = map(fromKey, toKey).values().iterator();

            return StreamSupport
                    .stream(Spliterators.spliteratorUnknownSize(
                            mergeTwo(ssTableRanges, memoryRange),
                            Spliterator.ORDERED),
                            false
                    )
                    .filter(record -> !record.isTombstone())
                    .iterator();
        }
    }

    @Override
    public void upsert(Record record) throws UncheckedIOException {
        synchronized (this) {
            memoryConsumption += sizeOf(record);
            if (memoryConsumption > LIMIT) {
                try {
                    flush();
                    memoryConsumption = 0;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
        memoryConsumption += sizeOf(record);
        memoryStorage.put(record.getKey(), record);
    }

    @Override
    public void compact() throws IOException {
        synchronized (this) {
            Iterator<Record> iterator = range(null, null);

            SSTable ssTable = SSTable.save(iterator, config.getDir(), nextSSTableNumber++);

            Path indexPath = ssTable.getIndexPath();
            Path savePath = ssTable.getSavePath();

            String indexFile = indexPath.getFileName().toString();
            String saveFile = savePath.getFileName().toString();


            List<SSTable> filteredSSTables = ssTables.stream()
                    .filter(ssTable1 -> ssTable1.getIndexPath().getFileName().toString().compareTo(indexFile) != 0
                            && ssTable1.getSavePath().getFileName().toString().compareTo(saveFile) != 0)
                    .collect(Collectors.toList());

            for (SSTable filteredSSTable : filteredSSTables) {
                filteredSSTable.close();
            }

            try (Stream<Path> paths = Files.walk(config.getDir())) {
                paths.filter(Files::isRegularFile)
                        .map(Path::toFile)
                        .filter(file -> file.getName().compareTo(indexFile) != 0 && file.getName().compareTo(saveFile) != 0)
                        .forEach(File::delete);
            }

            ssTables.clear();
            ssTables.add(ssTable);

            Files.move(ssTable.getIndexPath(), config.getDir().resolve(SSTable.FIRST_INDEX_FILE), StandardCopyOption.ATOMIC_MOVE);
            Files.move(ssTable.getSavePath(), config.getDir().resolve(SSTable.FIRST_SAVE_FILE), StandardCopyOption.ATOMIC_MOVE);
        }
    }

    @Override
    public void close() throws IOException {

        if (memoryConsumption > 0) {
            flush();
        }

        closeSSTables();
    }

    private void closeSSTables() throws IOException {
        for (SSTable ssTable : ssTables) {
            ssTable.close();
        }
    }

    private Map<ByteBuffer, Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {

        if (fromKey == null && toKey == null) {
            return memoryStorage;
        } else if (fromKey == null) {
            return memoryStorage.headMap(toKey);
        } else if (toKey == null) {
            return memoryStorage.tailMap(fromKey);
        } else {
            return memoryStorage.subMap(fromKey, toKey);
        }
    }

    private void flush() throws IOException {
        SSTable ssTable = SSTable.save(
                memoryStorage.values().iterator(),
                config.getDir(),
                nextSSTableNumber++
        );

        ssTables.add(ssTable);
        memoryStorage.clear();

    }

    private Iterator<Record> ssTableRanges(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey)
            throws IOException {
        List<Iterator<Record>> iterators = new ArrayList<>(ssTables.size());

        for (SSTable ssTable : ssTables) {
            iterators.add(ssTable.range(fromKey, toKey));
        }
        return merge(iterators);
    }

    private int sizeOf(Record record) {
        return record.getKey().remaining()
                + (record.isTombstone() ? 0 : record.getKey().remaining()) + Integer.BYTES * 2;
    }

    /**
     * Method that merge iterators and return iterator.
     *
     * @param iterators is list of iterators to merge
     * @return merged iterators
     */
    public static Iterator<Record> merge(List<Iterator<Record>> iterators) {

        if (iterators.isEmpty()) {
            return Collections.emptyIterator();
        } else if (iterators.size() == 1) {
            return iterators.get(0);
        } else if (iterators.size() == 2) {
            return mergeTwo(iterators.get(0), iterators.get(1));
        }

        Iterator<Record> left = merge(iterators.subList(0, iterators.size() / 2));
        Iterator<Record> right = merge(iterators.subList(iterators.size() / 2, iterators.size()));

        return mergeTwo(left, right);
    }

    private static Iterator<Record> mergeTwo(Iterator<Record> leftIterator, Iterator<Record> rightIterator) {
        return new MergeIterator(leftIterator, rightIterator);
    }
}

