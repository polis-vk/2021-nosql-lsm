package ru.mail.polis.lsm;

import java.nio.file.Path;

public class DAOConfig {

    private final Path dir;

    public DAOConfig(Path dir) {
        this.dir = dir;
    }

    public Path getDir() {
        return dir;
    }
}