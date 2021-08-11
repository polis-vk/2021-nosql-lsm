package units;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.mail.polis.lsm.Record;

import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static utils.Utils.*;

public class RecordTest {
    @Test
    void testSizeMethod(@TempDir Path dir) {
        ByteBuffer key = wrap("SOME_KEY");
        ByteBuffer value = wrap("SOME_VALUE");

        Record record = Record.of(key, value);
        long size = record.getSize();
        assertEquals(26L, size);

        assertEquals(12, Record.tombstone(key).getSize());
    }

}
