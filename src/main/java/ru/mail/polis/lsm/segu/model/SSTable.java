package ru.mail.polis.lsm.segu.model;

import ru.mail.polis.lsm.Record;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.SortedMap;

public class SSTable {

    public static final String FILE_PREFIX = "file_";
    public static final String INDEX_FILE_PREFIX = "idx_";

    private final Path indexFilePath;
    private final Path filePath;

    private final SortedMap<ByteBuffer, Record> storage;


    public SSTable(Path filePath, Path indexFilePath, SortedMap<ByteBuffer, Record> storage) {
        this.filePath = filePath;
        this.indexFilePath = indexFilePath;
        this.storage = storage;
    }

    public Path getIndexFilePath() {
        return indexFilePath;
    }

    public Path getFilePath() {
        return filePath;
    }

    public SortedMap<ByteBuffer, Record> getStorage() {
        return storage;
    }

}
