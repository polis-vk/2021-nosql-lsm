package ru.mail.polis.lsm.segu.model;

import java.nio.file.Path;
import java.util.Objects;

public class SSTablePath {
    private final Path filePath;
    private final Path indexFilePath;

    public SSTablePath(Path filePath, Path indexFilePath) {
        this.filePath = filePath;
        this.indexFilePath = indexFilePath;
    }

    public Path getFilePath() {
        return filePath;
    }

    public Path getIndexFilePath() {
        return indexFilePath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SSTablePath that = (SSTablePath) o;
        return Objects.equals(filePath, that.filePath) && Objects.equals(indexFilePath, that.indexFilePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filePath, indexFilePath);
    }
}
