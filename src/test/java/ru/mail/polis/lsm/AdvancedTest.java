package ru.mail.polis.lsm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import static ru.mail.polis.lsm.Utils.assertEquals;
import static ru.mail.polis.lsm.Utils.generateMap;
import static ru.mail.polis.lsm.Utils.key;
import static ru.mail.polis.lsm.Utils.value;

public class AdvancedTest {

    @Test
    void close(@TempDir Path data) throws IOException {
        Iterator<Record> iterator;

        Map<ByteBuffer, ByteBuffer> map = generateMap(0, 1000);

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.upsert(Record.of(key(1), value(1)));

            map.forEach((k, v) -> dao.upsert(Record.of(k, v)));

            iterator = dao.range(null, null);
        }

        assertEquals(iterator, new TreeMap<>(map).entrySet());
    }

}
