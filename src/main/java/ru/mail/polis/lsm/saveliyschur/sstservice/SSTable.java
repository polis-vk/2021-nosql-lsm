package ru.mail.polis.lsm.saveliyschur.sstservice;

import java.nio.ByteBuffer;
import java.nio.file.Path;

public class SSTable {
    public static final String NAME = "sstable_";
    public static final String EXTENSION = ".sst";

    private final Path path;

    public SSTable(Path path) {
        this.path = path;
    }

    public Path getPath() {
        return path;
    }
}
