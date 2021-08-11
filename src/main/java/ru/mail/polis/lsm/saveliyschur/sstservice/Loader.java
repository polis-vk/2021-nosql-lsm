package ru.mail.polis.lsm.saveliyschur.sstservice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static ru.mail.polis.lsm.saveliyschur.sstservice.SSTable.SSTABLE_FILE_PREFIX;

public class Loader {

    private static final Logger log = LoggerFactory.getLogger(Loader.class);

    public static List<ISSTable> loadFromDir(Path dir) throws IOException {
        log.info("Load SSTables");

        List<ISSTable> result = new ArrayList<>();
        for (int i = 0; ; i++) {
            Path file = dir.resolve(SSTABLE_FILE_PREFIX + i);
            if (!Files.exists(file)) {
                return result;
            }
            result.add(new SSTable(file));
        }
    }
}
