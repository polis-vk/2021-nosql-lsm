package ru.mail.polis.lsm.danilafedorov;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.*;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

public class SSTable {

    private static final Method CLEAN;
    private static final Integer NULL_SIZE = -1;
    private static final String TEMP_FILE_ENDING = "_temp";

    private final SortedMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    private final MappedByteBuffer mmap;

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
                    .filter(path -> !path.getFileName().endsWith(TEMP_FILE_ENDING))
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
        try (FileChannel channel = FileChannel.open(
                pathTemp,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {
            final ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);

            while (iterator.hasNext()) {
                final Record record = iterator.next();
                write(channel, record.getKey(), size);
                write(channel, record.getValue(), size);
            }

            channel.force(false);
        }

        Files.move(pathTemp, path, StandardCopyOption.ATOMIC_MOVE);

        return new SSTable(path);
    }

    public SSTable(final Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            mmap = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
            while (mmap.hasRemaining()) {
                Record record;

                ByteBuffer key = Objects.requireNonNull(read());
                ByteBuffer value = read();
                if (value == null) {
                    record = Record.tombstone(key);
                } else {
                    record = Record.of(key, value);
                }
                storage.put(key, record);
            }
        }
    }

    public Iterator<Record> range(@Nullable final ByteBuffer fromKey, @Nullable final ByteBuffer toKey) {
        return DAO.getSubMap(fromKey, toKey, storage).values().iterator();
    }

    public void close() throws IOException {
        if (mmap != null) {
            try {
                CLEAN.invoke(null, mmap);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IOException(e);
            }
        }
    }

    private static void write(WritableByteChannel channel, @Nullable ByteBuffer value, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        int size = value == null ? NULL_SIZE : value.remaining();
        tmp.putInt(size);

        tmp.position(0);
        channel.write(tmp);

        if (value != null) {
            channel.write(value);
        }
    }

    @Nullable
    private ByteBuffer read() {
        int size = mmap.getInt();
        if (size == NULL_SIZE) {
            return null;
        }

        ByteBuffer value = mmap.slice().limit(size).asReadOnlyBuffer();
        mmap.position(mmap.position() + size);
        return value;
    }

    private static Integer getFileOrder(Path path) {
        String orderString = path.getFileName().toString().substring(LsmDAO.FILE_NAME.length());

        return Integer.parseInt(orderString);
    }
}
