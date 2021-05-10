package ru.mail.polis.lsm;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.mail.polis.lsm.ponomarev_stepan.InMemoryDAO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static ru.mail.polis.lsm.Utils.wrap;

class MusicTest {
    private static final char DELIMITER = '\0';
    private static final char DELIMITER_FOR_SUFFIX = DELIMITER + 1;

    private DAO dao;

    @BeforeEach
    void start(@TempDir Path dir) throws IOException {
        dao = new InMemoryDAO();
    }

    @Test
    void database() {
        // Fill music database
        dao.upsert(record(trackFrom("Ar1", "Al11", "T111"), 15));
        dao.upsert(record(trackFrom("Ar1", "Al11", "T112"), 24));
        dao.upsert(record(trackFrom("Ar1", "Al12", "T111"), 33));
        dao.upsert(record(trackFrom("Ar1", "Al12", "T1111"), 49));
        dao.upsert(record(trackFrom("Ar1", "Al12", "T112"), 50));
        dao.upsert(record(trackFrom("Ar2", "Al21", "T211"), 62));
        dao.upsert(record(trackFrom("Ar2", "Al21", "T212"), 78));

        // Re-open music database
        reopen();

        // Artists
        assertRangeSize(artistFrom("Ar1"), 5);
        assertRangeSize(artistFrom("Ar2"), 2);

        // Albums
        assertRangeSize(albumFrom("Ar1", "Al11"), 2);
        assertRangeSize(albumFrom("Ar1", "Al12"), 3);
        assertRangeSize(albumFrom("Ar2", "Al21"), 2);
    }

    private void reopen() {
        // TODO next time
    }

    private void assertRangeSize(String suffix, int count) {
        Iterator<Record> range = dao.range(
                wrap(suffix + DELIMITER),
                wrap(suffix + DELIMITER_FOR_SUFFIX)
        );

        int size = 0;
        while (range.hasNext()) {
            size++;
            range.next();
        }

        assertEquals(count, size);
    }

    private static String artistFrom(String artist) {
        assert artist.indexOf(DELIMITER) == -1;
        assert artist.indexOf(DELIMITER_FOR_SUFFIX) == -1;

        return artist;
    }

    private static String albumFrom(String artist, String album) {
        assert artist.indexOf(DELIMITER) == -1;
        assert artist.indexOf(DELIMITER_FOR_SUFFIX) == -1;
        assert album.indexOf(DELIMITER) == -1;
        assert album.indexOf(DELIMITER_FOR_SUFFIX) == -1;

        return artist + DELIMITER + album;
    }

    private static String trackFrom(
            String artist,
            String album,
            String track
    ) {
        assert artist.indexOf(DELIMITER) == -1;
        assert artist.indexOf(DELIMITER_FOR_SUFFIX) == -1;
        assert album.indexOf(DELIMITER) == -1;
        assert album.indexOf(DELIMITER_FOR_SUFFIX) == -1;
        assert track.indexOf(DELIMITER) == -1;
        assert track.indexOf(DELIMITER_FOR_SUFFIX) == -1;

        return artist + DELIMITER + album + DELIMITER + track;
    }

    private static Record record(String track, int duration) {
        return Record.of(
                wrap(track),
                duration(duration)
        );
    }

    private static ByteBuffer duration(int seconds) {
        ByteBuffer result = ByteBuffer.allocate(4);
        result.putInt(seconds);
        result.rewind();
        return result;
    }

}
