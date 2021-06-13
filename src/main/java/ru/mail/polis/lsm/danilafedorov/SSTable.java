package ru.mail.polis.lsm.danilafedorov;

import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Stream;

public class SSTable implements Closeable {

    private static final Method CLEAN;
    private static final Integer NULL_SIZE = -1;
    private static final String TEMP_FILE_ENDING = "_temp";
    private static final String INDEX_FILE_ENDING = "_index";

    private final MappedByteBuffer mmap;
    private final Path indexPath;
    private final int[] indexes;

    static {
        try {
            Class<?> clas = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = clas.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    static ConcurrentLinkedDeque<SSTable> loadFromDir(Path dir) throws IOException {
        ConcurrentLinkedDeque<SSTable> ssTables = new ConcurrentLinkedDeque<>();
        try (Stream<Path> paths = Files.list(dir)) {
            Iterator<Path> it = paths
                    .filter(SSTable::isSSTableFile)
                    .sorted(Comparator.comparing(SSTable::getFileOrder))
                    .iterator();

            while (it.hasNext()) {
                Path path = it.next();
                ssTables.add(new SSTable(path));
            }
        }

        return ssTables;
    }

    static SSTable write(final Path path, final Iterator<Record> iterator) throws IOException {
        String name = path.getFileName().toString();
        Path pathTemp = path.resolveSibling(name + TEMP_FILE_ENDING);
        Path pathIndex = path.resolveSibling(name + INDEX_FILE_ENDING);
        try (FileChannel mainChannel =
                     FileChannel.open(
                             pathTemp,
                             StandardOpenOption.CREATE_NEW,
                             StandardOpenOption.WRITE,
                             StandardOpenOption.TRUNCATE_EXISTING
                     );
             FileChannel indexChannel =
                     FileChannel.open(
                             pathIndex,
                             StandardOpenOption.CREATE_NEW,
                             StandardOpenOption.WRITE,
                             StandardOpenOption.TRUNCATE_EXISTING
                     )
        ) {
            final ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            int position = 0;
            while (iterator.hasNext()) {
                final Record record = iterator.next();
                writeValue(mainChannel, record.getKey(), size);
                writeValue(mainChannel, record.getValue(), size);
                writeInt(indexChannel, position, size);
                position = (int) mainChannel.position();
            }

            mainChannel.force(false);
            indexChannel.force(false);
        }

        Files.move(pathTemp, path, StandardCopyOption.ATOMIC_MOVE);

        return new SSTable(path);
    }

    /**
     * Opens SSTable located in memory.
     *
     * @param path contains path to SSTable
     * @throws IOException If an input exception occurred
     */
    public SSTable(final Path path) throws IOException {
        String name = path.getFileName().toString();
        indexPath = path.resolveSibling(name + INDEX_FILE_ENDING);

        indexes = getIndexes();
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            mmap = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        }
    }

    /**
     * Provides iterator for memory data.
     *
     * @param fromKey first record's key will be greater or equal to {@code fromKey}
     * @param toKey last record's key will be less then {@code toKey}
     * @return iterator for appropriate records
     */
    public Iterator<Record> range(@Nullable final ByteBuffer fromKey, @Nullable final ByteBuffer toKey) {
        int fromIndex = fromKey == null ? 0 : findKeyIndex(fromKey);
        if (fromIndex == indexes.length) {
            return SSTableIterator.emptyIterator();
        }

        int toIndex = toKey == null ? indexes.length : findKeyIndex(toKey);

        ByteBuffer buffer = mmap.position(indexes[fromIndex]).slice();
        if (toIndex < indexes.length) {
            buffer = buffer.limit(indexes[toIndex] - indexes[fromIndex]);
        }
        buffer = buffer.asReadOnlyBuffer();
        return new SSTableIterator(buffer);
    }

    @Override
    public void close() throws IOException {
        if (mmap != null) {
            try {
                CLEAN.invoke(null, mmap);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IOException(e);
            }
        }
    }

    private static boolean isSSTableFile(Path path) {
        String name = path.getFileName().toString();
        return !(name.endsWith(INDEX_FILE_ENDING) || name.endsWith(TEMP_FILE_ENDING));
    }

    private static Integer getFileOrder(Path path) {
        String orderString = path.getFileName().toString().substring(LsmDAO.FILE_NAME.length());
        return Integer.parseInt(orderString);
    }

    private int[] getIndexes() throws IOException {
        try (FileChannel channel = FileChannel.open(indexPath, StandardOpenOption.READ)) {
            MappedByteBuffer buf = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
            byte[] bytes = new byte[buf.limit()];
            buf.get(bytes);
            IntBuffer intBuf = ByteBuffer.wrap(bytes).asIntBuffer();
            int[] ints = new int[intBuf.remaining()];
            intBuf.get(ints);
            CLEAN.invoke(null, buf);
            return ints;
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private int findKeyIndex(ByteBuffer key) {
        ByteBuffer buffer = mmap.duplicate();

        int first = 0;
        int last = indexes.length;

        while ((last - first) > 0) {
            int middle = (first + last) / 2;
            buffer.position(indexes[middle]);
            ByteBuffer current = checkValue(buffer);
            int compareResult = key.compareTo(current);

            if (compareResult < 0) {
                last = middle;
            } else if (compareResult > 0) {
                first = middle + 1;
            } else {
                return middle;
            }
        }
        return last;
    }

    private static void writeValue(WritableByteChannel channel,
                                   @Nullable ByteBuffer value,
                                   ByteBuffer tmp) throws IOException {
        int size = value == null ? NULL_SIZE : value.remaining();
        writeInt(channel, size, tmp);

        if (value != null) {
            channel.write(value);
        }
    }

    private static void writeInt(WritableByteChannel channel, int value, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        tmp.putInt(value);
        tmp.position(0);
        channel.write(tmp);
    }

    private ByteBuffer checkValue(ByteBuffer buf) {
        int size = buf.getInt();
        return buf.slice().limit(size).asReadOnlyBuffer();
    }

    static class SSTableIterator implements Iterator<Record> {

        final ByteBuffer buffer;

        static public SSTableIterator emptyIterator() {
            return new SSTableIterator(ByteBuffer.allocate(0));
        }

        SSTableIterator(ByteBuffer mmap) {
            this.buffer = mmap;
        }

        @Override
        public boolean hasNext() {
            return buffer.hasRemaining();
        }

        @Override
        public Record next() {
            if (!hasNext()) {
                return null;
            }

            ByteBuffer key = Objects.requireNonNull(read());
            ByteBuffer value = read();
            return value == null ? Record.tombstone(key) : Record.of(key, value);
        }

        @Nullable
        private ByteBuffer read() {
            int size = buffer.getInt();
            if (size == NULL_SIZE) {
                return null;
            }

            ByteBuffer value = buffer.slice().limit(size).asReadOnlyBuffer();
            buffer.position(buffer.position() + size);
            return value;
        }
    }
}
