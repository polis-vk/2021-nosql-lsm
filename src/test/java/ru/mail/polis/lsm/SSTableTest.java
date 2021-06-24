package ru.mail.polis.lsm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.mail.polis.lsm.saveliyschur.sstservice.SSTable;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SSTableTest {

    @Test
    void compareTest(@TempDir Path data) {
        SSTable ssTable = new SSTable(data.resolve(SSTable.NAME + 0 + SSTable.EXTENSION));
        SSTable ssTable1 = new SSTable(data.resolve(SSTable.NAME + 1 + SSTable.EXTENSION));

        assertEquals(-1, ssTable.compareTo(ssTable1));
    }

    @Test
    void testSort(@TempDir Path data) {
        List<SSTable> ssTables = Collections.synchronizedList(new ArrayList<>());
        for (int i = 3; i < 10; i++) {
            ssTables.add(creator(data, i));
        }
        SSTable ssTableWith0 = creator(data, 0);
        ssTables.add(ssTableWith0);

        List<SSTable> answer = ssTables.stream().sorted().collect(Collectors.toList());
        assertEquals(ssTableWith0.getPath(), answer.get(0).getPath());
    }

    private SSTable creator(Path data, int i) {
        return new SSTable(data.resolve(SSTable.NAME + i + SSTable.EXTENSION));
    }
}
