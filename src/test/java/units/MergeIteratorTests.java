package units;

import org.junit.jupiter.api.Test;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.lsm.saveliyschur.utils.PeekingIterator;
import ru.mail.polis.lsm.saveliyschur.utils.UtilsIterator;
import utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;
import static utils.Utils.wrap;

public class MergeIteratorTests {

    @Test
    void nextTestPeekingIterator() {
        ByteBuffer key = wrap("KEY");
        ByteBuffer value = wrap("VALUE_1");
        Record record = Record.of(key, value);

        List<Record> one = new ArrayList<>(1);
        one.add(record);
        Iterator<Record> iterator = one.iterator();
        PeekingIterator peekingIterator = new PeekingIterator(iterator);

        assertTrue(peekingIterator.hasNext());
        Utils.assertEqualsRecords(record, peekingIterator.peek());

        Record recordNext = peekingIterator.next();

        assertFalse(peekingIterator.hasNext());
        Utils.assertEqualsRecords(record, recordNext);
        try {
            peekingIterator.next();
            throw new IllegalStateException("Element exists, bur expected empty iterator");
        } catch (NoSuchElementException ignored) {}
    }


    @Test
    void mergeSimple() throws IOException {
        List<Record> one = new ArrayList<>(1);
        List<Record> two = new ArrayList<>(1);

        ByteBuffer key = wrap("KEY");
        ByteBuffer value = wrap("VALUE_1");

        one.add(Record.of(key, value));
        two.add(Record.tombstone(key));

        PeekingIterator oneIterator = new PeekingIterator(one.iterator());
        PeekingIterator twoIterator = new PeekingIterator(two.iterator());

        Iterator<Record> answer = UtilsIterator.mergeTwo(oneIterator, twoIterator);
        assertTrue(answer.hasNext());
        assertNull(answer.next().getValue());
    }

    @Test
    void mergeSimpleTwo() throws IOException {
        List<Record> one = new ArrayList<>(1);
        List<Record> two = new ArrayList<>(1);

        ByteBuffer key = wrap("KEY");
        ByteBuffer value = wrap("VALUE_1");

        one.add(Record.of(key, value));
        two.add(Record.tombstone(key));

        PeekingIterator oneIterator = new PeekingIterator(one.iterator());
        PeekingIterator twoIterator = new PeekingIterator(two.iterator());

        Iterator<Record> answer = UtilsIterator.mergeTwo(twoIterator, oneIterator);
        assertTrue(answer.hasNext());
        assertEquals(value, answer.next().getValue());
    }
}
