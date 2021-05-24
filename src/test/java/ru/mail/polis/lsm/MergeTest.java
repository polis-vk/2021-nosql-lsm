package ru.mail.polis.lsm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opentest4j.AssertionFailedError;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static ru.mail.polis.lsm.Utils.key;
import static ru.mail.polis.lsm.Utils.sizeBasedRandomData;
import static ru.mail.polis.lsm.Utils.valueWithSuffix;

class MergeTest {

    @Test
    void hugeValues(@TempDir Path data) throws IOException {
        // Reference value
        int size = 1024 * 1024;
        byte[] suffix = sizeBasedRandomData(size);
        int count = 128;
        int recordsOverall = (int) (TestDaoWrapper.MAX_HEAP / size + 1);
        int records = recordsOverall / (count / 2);

        TreeMap<String, Integer> expected = new TreeMap<>();

        for (int prefix = 0; prefix < count; prefix++) {
            // Create, fill and close storage
            try (DAO dao = createDAO(data, prefix)) {
                for (int i = 0; i < records; i++) {
                    int keyIndex = prefix * records / 2 + i;
                    int valueIndex = prefix * records + i;

                    ByteBuffer key = key(keyIndex);
                    ByteBuffer value = valueWithSuffix(valueIndex, suffix);
                    dao.upsert(Record.of(key, value));
                    assertEquals(value, dao.range(key, null).next().getValue());

                    expected.put(String.valueOf(keyIndex), valueIndex);
                }
            }
        }

        IOException e = null;
        List<DAO> dao = new ArrayList<>();
        try {
            for (int prefix = 0; prefix < count; prefix++) {
                dao.add(createDAO(data, prefix));
            }

            List<Iterator<Record>> iterators = dao.stream().map(d -> d.range(null, null)).collect(Collectors.toList());
            List<Iterator<Record>> cycledIterators = dao.stream().map(d -> {
                List<Record> list = new ArrayList<Record>();
                d.range(null, null).forEachRemaining(list::add);
                return new CycledIterator(list);
            }).collect(Collectors.toList());

            Iterator<Record> iterator = DAO.merge(iterators);
            Iterator<Record> cycledIterator = DAO.merge(cycledIterators);
            for (Map.Entry<String, Integer> entry : expected.entrySet()) {
                if (!iterator.hasNext()) {
                    throw new AssertionFailedError("Iterator ended on key " + entry.getKey());
                }
                if (!cycledIterator.hasNext()) {
                    throw new AssertionFailedError("Cycled iterator ended on key " + entry.getKey());
                }
                Record next = iterator.next();
                Record cycledNext = cycledIterator.next();
                assertEquals(Utils.toString(key(Integer.parseInt(entry.getKey()))), Utils.toString(next.getKey()));
                assertEquals(Utils.toString(valueWithSuffix(entry.getValue(), suffix)), Utils.toString(next.getValue()));

                assertEquals(Utils.toString(key(Integer.parseInt(entry.getKey()))), Utils.toString(cycledNext.getKey()));
                assertEquals(Utils.toString(valueWithSuffix(entry.getValue(), suffix)), Utils.toString(cycledNext.getValue()));
            }
            if (iterator.hasNext()) {
                throw new AssertionFailedError("Iterator has extra record with key " + iterator.next().getKey());
            }
            if (cycledIterator.hasNext()) {
                throw new AssertionFailedError("Cycled iterator has extra record with key " + iterator.next().getKey());
            }
        } catch (OutOfMemoryError ez) {
            throw new RuntimeException(ez);  // NEVER DO IT IN PRODUCTION CODE!!!
        } finally {
            for (DAO d : dao) {
                try {
                    d.close();
                } catch (Exception ex) {
                    if (e == null) {
                        e = new IOException("Can't close DAO");
                    }
                    e.addSuppressed(e);
                }
            }
        }

        if (e != null) {
            throw e;
        }
    }

    private DAO createDAO(@TempDir Path data, int prefix) throws IOException {
        Path child = data.resolve("child_" + prefix);
        Files.createDirectories(child);
        return TestDaoWrapper.create(new DAOConfig(child));
    }

    /**
     * Cycled iterator class for infinite iterator test.
     */
    private static class CycledIterator implements Iterator<Record> {
        private final List<Record> data;
        private int current;
        public CycledIterator(List<Record> data) {
            this.data = data;
        }
        @Override
        public boolean hasNext() {
            return data.size() > 0;
        }

        @Override
        public Record next() {
            return data.get((current++) % data.size());
        }

        @Override
        public void remove() {
            if (current - 1 < 0) {
                data.remove(data.size() - 1);
            } else {
                data.remove(current - 1);
            }
        }
    }
}
