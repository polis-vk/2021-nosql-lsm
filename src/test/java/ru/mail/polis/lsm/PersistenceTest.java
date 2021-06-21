package ru.mail.polis.lsm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.mail.polis.lsm.shabinsky.SSTable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.*;
import static ru.mail.polis.lsm.Utils.*;

class PersistenceTest {
    @Test
    void fs(@TempDir Path data) throws IOException {
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.upsert(Record.of(key(1), value(1)));
        }

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            Iterator<Record> range = dao.range(null, null);
            assertTrue(range.hasNext());

            Record record = range.next();
            assertEquals(key(1), record.getKey());
            assertEquals(value(1), record.getValue());
        }

        recursiveDelete(data);

        assertFalse(Files.exists(data));
        Files.createDirectory(data);

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            assertFalse(dao.range(null, null).hasNext());
        }
    }

    @Test
    void remove(@TempDir Path data) throws IOException {
        // Reference value
        ByteBuffer key = wrap("SOME_KEY");
        ByteBuffer value = wrap("SOME_VALUE");

        // Create dao and fill data
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.upsert(Record.of(key, value));
            Iterator<Record> range = dao.range(null, null);

            assertTrue(range.hasNext());
            assertEquals(value, range.next().getValue());
        }

        // Load data and check
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            Iterator<Record> range = dao.range(null, null);
            assertTrue(range.hasNext());
            assertEquals(value, range.next().getValue());

            // Remove data and flush
            dao.upsert(Record.tombstone(key));
        }

        // Load and check not found
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            Iterator<Record> range = dao.range(null, null);

            assertFalse(range.hasNext());
        }
    }

    @Test
    void replaceWithClose(@TempDir Path data) throws Exception {
        ByteBuffer key = wrap("KEY");
        ByteBuffer value = wrap("VALUE_1");
        ByteBuffer value2 = wrap("VALUE_2");

        // Initial insert
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.upsert(Record.of(key, value));

            Iterator<Record> range = dao.range(null, null);
            assertTrue(range.hasNext());
            assertEquals(value, range.next().getValue());
        }

        // Reopen
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            Iterator<Record> range = dao.range(null, null);
            assertTrue(range.hasNext());
            assertEquals(value, range.next().getValue());

            // Replace
            dao.upsert(Record.of(key, value2));

            Iterator<Record> range2 = dao.range(null, null);
            assertTrue(range2.hasNext());
            assertEquals(value2, range2.next().getValue());
        }

        // Reopen
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            // Last value should win
            Iterator<Record> range2 = dao.range(null, null);
            assertTrue(range2.hasNext());
            assertEquals(value2, range2.next().getValue());
        }
    }

    @Test
    void burn(@TempDir Path data) throws IOException {
        ByteBuffer key = wrap("FIXED_KEY");

        int overwrites = 100;
        for (int i = 0; i < overwrites; i++) {
            ByteBuffer value = value(i);
            try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
                dao.upsert(Record.of(key, value));
                assertEquals(value, dao.range(key, null).next().getValue());
            }

            // Check
            try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
                assertEquals(value, dao.range(key, null).next().getValue());
            }
        }
    }

    @Test
    void hugeRecords(@TempDir Path data) throws IOException {
        // Reference value
        int size = 1024 * 1024;
        byte[] suffix = sizeBasedRandomData(size);
        int recordsCount = (int) (TestDaoWrapper.MAX_HEAP * 15 / size);

        prepareHugeDao(data, recordsCount, suffix);

        // Check
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            Iterator<Record> range = dao.range(null, null);

            for (int i = 0; i < recordsCount; i++) {
                verifyNext(suffix, range, i);
            }

            assertFalse(range.hasNext());
        }
    }

    @Test
    void hugeRecordsSearch(@TempDir Path data) throws IOException {
        // Reference value
        int size = 1024 * 1024;
        byte[] suffix = sizeBasedRandomData(size);
        int recordsCount = (int) (TestDaoWrapper.MAX_HEAP * 15 / size);

        prepareHugeDao(data, recordsCount, suffix);

        // Check
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            int searchStep = 4;

            for (int i = 0; i < recordsCount / searchStep; i++) {
                ByteBuffer keyFrom = keyWithSuffix(i * searchStep, suffix);
                ByteBuffer keyTo = keyWithSuffix(i * searchStep + searchStep, suffix);

                Iterator<Record> range = dao.range(keyFrom, keyTo);
                for (int j = 0; j < searchStep; j++) {
                    verifyNext(suffix, range, i * searchStep + j);
                }
                assertFalse(range.hasNext());
            }
        }
    }

    @Test
    void compactSimple(@TempDir Path data) throws IOException {
        ByteBuffer key = wrap("FIXED_KEY");

        int overwrites = 100;
        for (int i = 0; i < overwrites; i++) {
            ByteBuffer value = value(i);
            try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
                dao.upsert(Record.of(key, value));
                assertEquals(value, dao.range(key, null).next().getValue());
            }

            // Check
            try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
                assertEquals(value, dao.range(key, null).next().getValue());
            }
        }

        int countOld = getCountFiles(data);
        long sizeOld = getSizeKBFiles(data);
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.compact();
        }
        int countNew = getCountFiles(data);
        long sizeNew = getSizeKBFiles(data);

        assertTrue(
            (countOld / countNew) >= overwrites - 1
                && (countOld / countNew) <= overwrites + 1);

        assertTrue(
            (sizeOld / sizeNew) >= overwrites - 1
                && (sizeOld / sizeNew) <= overwrites + 1);
    }

    @Test
    void compactNoChanges(@TempDir Path data) throws IOException {
        // Reference value
        int size = 1024 * 1024;
        byte[] suffix = sizeBasedRandomData(size);
        int recordsCount = (int) (TestDaoWrapper.MAX_HEAP * 15 / size);

        prepareHugeDao(data, recordsCount, suffix);

        // Check
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            Iterator<Record> range = dao.range(null, null);

            for (int i = 0; i < recordsCount; i++) {
                verifyNext(suffix, range, i);
            }

            assertFalse(range.hasNext());

            int countOld = getCountFiles(data);
            long sizeOld = getSizeKBFiles(data);
            dao.compact();
            int countNew = getCountFiles(data);
            long sizeNew = getSizeKBFiles(data);
            assertEquals(countOld, countNew);
            assertEquals(sizeOld, sizeNew);

            range = dao.range(null, null);
            for (int i = 0; i < recordsCount; i++) {
                verifyNext(suffix, range, i);
            }
        }
    }

    private int getCountFiles(@TempDir Path dir) {
        int i = 0;
        for (; ; i++) {
            Path saveFileName = dir.resolve(SSTable.sstableName(i) + SSTable.SAVE);
            if (!Files.exists(saveFileName)) break;
        }
        return i;
    }

    private long getSizeKBFiles(@TempDir Path dir) throws IOException {
        long size = 0;
        for (int i = 0; ; i++) {
            Path saveFileName = dir.resolve(SSTable.sstableName(i) + SSTable.SAVE);
            if (!Files.exists(saveFileName)) break;
            size += Files.size(saveFileName);
        }
        return size;
    }

    private void verifyNext(byte[] suffix, Iterator<Record> range, int index) {
        ByteBuffer key = keyWithSuffix(index, suffix);
        ByteBuffer value = valueWithSuffix(index, suffix);

        Record next = range.next();

        assertEquals(key, next.getKey());
        assertEquals(value, next.getValue());
    }

    private void prepareHugeDao(@TempDir Path data, int recordsCount, byte[] suffix) throws IOException {
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            for (int i = 0; i < recordsCount; i++) {
                ByteBuffer key = keyWithSuffix(i, suffix);
                ByteBuffer value = valueWithSuffix(i, suffix);

                dao.upsert(Record.of(key, value));
            }
        }
    }

}
