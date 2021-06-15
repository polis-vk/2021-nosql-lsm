package ru.mail.polis.lsm.segu.model;

import java.nio.file.Path;

public class SSTable {

    public static final String FILE_PREFIX = "file_";
    public static final String INDEX_FILE_PREFIX = "idx_";

    private final Path indexFilePath;
    private final Path filePath;


    public SSTable(Path filePath, Path indexFilePath) {
        this.filePath = filePath;
        this.indexFilePath = indexFilePath;
    }

    public SSTable(Path filePath) {
        this(filePath, null);
    }

    public Path getIndexFilePath() {
        return indexFilePath;
    }

    public Path getFilePath() {
        return filePath;
    }

}
