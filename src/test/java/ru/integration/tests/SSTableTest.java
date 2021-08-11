package ru.integration.tests;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.TestDaoWrapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;
import static utils.Utils.wrap;

public class SSTableTest {

    @Test
    void test(@TempDir Path data) throws IOException {
        ByteBuffer key = wrap("KEY_1");
        ByteBuffer key2 = wrap("KEY_2");
        ByteBuffer value = wrap("VALUE_1");
        ByteBuffer value2 = wrap("VALUE_2");

        // Initial insert
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.upsert(Record.of(key, value));

            Iterator<Record> range = dao.range(null, null);
            assertTrue(range.hasNext());
            assertEquals(value, range.next().getValue());
        }

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.upsert(Record.of(key, value2));

            Iterator<Record> range = dao.range(null, null);
            assertTrue(range.hasNext());
            assertEquals(value2, range.next().getValue());
        }

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.upsert(Record.of(key2, value));

            Iterator<Record> range = dao.range(null, null);
            assertTrue(range.hasNext());
            assertEquals(value2, range.next().getValue());
            assertTrue(range.hasNext());
            assertEquals(value, range.next().getValue());
            assertFalse(range.hasNext());
        }
    }

    @Test
    void smallRange(@TempDir Path data) throws IOException {
        ByteBuffer key = wrap("KEY_1");
        ByteBuffer key2 = wrap("KEY_2");
        ByteBuffer value = wrap("VALUE_1");
        ByteBuffer value2 = wrap("VALUE_2");

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.upsert(Record.of(key, value));

            Iterator<Record> iterator = dao.range(null, null);
            assertEquals(value, iterator.next().getValue());
        }
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {

            dao.upsert(Record.of(key2, value2));

            Iterator<Record> iterator2 = dao.range(null, null);

            assertEquals(key, iterator2.next().getKey());
            assertEquals(key2, iterator2.next().getKey());
        }
    }

    @Test
    void smallRangeInMemory(@TempDir Path data) throws IOException {
        ByteBuffer key = wrap("KEY_1");
        ByteBuffer key2 = wrap("KEY_2");
        ByteBuffer value = wrap("VALUE_1");
        ByteBuffer value2 = wrap("VALUE_2");

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.upsert(Record.of(key, value));

            Iterator<Record> iterator = dao.range(null, null);
            assertEquals(value, iterator.next().getValue());

            dao.upsert(Record.of(key2, value2));

            Iterator<Record> iterator2 = dao.range(null, null);

            assertEquals(key, iterator2.next().getKey());
            assertEquals(key2, iterator2.next().getKey());
        }
    }

    @Test
    void smallRangeInOneSSTable(@TempDir Path data) throws IOException {
        ByteBuffer key = wrap("KEY_1");
        ByteBuffer key2 = wrap("KEY_2");
        ByteBuffer value = wrap("VALUE_1");
        ByteBuffer value2 = wrap("VALUE_2");

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.upsert(Record.of(key, value));

            Iterator<Record> iterator = dao.range(null, null);
            assertEquals(value, iterator.next().getValue());

            dao.upsert(Record.of(key2, value2));

            Iterator<Record> iterator2 = dao.range(null, null);

            assertEquals(key, iterator2.next().getKey());
            assertEquals(key2, iterator2.next().getKey());
        }

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {

            Iterator<Record> iterator2 = dao.range(null, null);

            assertEquals(key, iterator2.next().getKey());
            assertEquals(key2, iterator2.next().getKey());
        }

    }

    @Test
    void smallRangeInTwoSSTable(@TempDir Path data) throws IOException {
        ByteBuffer key = wrap("KEY_1");
        ByteBuffer key2 = wrap("KEY_2");
        ByteBuffer value = wrap("VALUE_1");
        ByteBuffer value2 = wrap("VALUE_2");

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.upsert(Record.of(key, value));

            Iterator<Record> iterator = dao.range(null, null);
            assertEquals(value, iterator.next().getValue());
        }

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.upsert(Record.of(key2, value2));

            Iterator<Record> iterator2 = dao.range(null, null);

            assertEquals(key, iterator2.next().getKey());
            assertEquals(key2, iterator2.next().getKey());
        }

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {

            Iterator<Record> iterator2 = dao.range(null, null);

            assertEquals(key, iterator2.next().getKey());
            assertEquals(key2, iterator2.next().getKey());
        }
    }
}
