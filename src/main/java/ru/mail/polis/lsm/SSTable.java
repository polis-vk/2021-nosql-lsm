package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class SSTable implements Closeable {

    private static final Method CLEAN;

    static {
        try {
            Class<?> fileChannelImplClass = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = fileChannelImplClass.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    private MappedByteBuffer mmap;

    static List<SSTable> loadFromDir(Path dir) throws IOException {
            return Files.list(dir)
                    .filter(path -> path.getFileName().toString().startsWith("file_"))
                    .sorted(Comparator.comparingInt(path -> Integer.parseInt(path
                            .getFileName().toString().substring(5))))
                    .map(path -> new SSTable(dir.resolve(path)))
                    .collect(Collectors.toList());
    }

    static SSTable write(Iterator<Record> records, Path file) throws IOException {
        Files.deleteIfExists(file);
        try (FileChannel fileChannel = FileChannel.open(
                file,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {
            ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            while (records.hasNext()) {
                Record record = records.next();

                writeInt(record.getKey(), fileChannel, size);
                if (record.isTombstone()) {
                    ByteBuffer nullbb = ByteBuffer.allocate(Integer.BYTES);
                    nullbb.putInt(-1);
                    nullbb.position(0);
                    fileChannel.write(nullbb);
                } else {
                    writeInt(record.getValue(), fileChannel, size);
                }
            }

            fileChannel.force(false);
        }

        return new SSTable(file);
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

    private static void writeInt(ByteBuffer value, WritableByteChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        tmp.putInt(value.remaining());
        tmp.position(0);
        channel.write(tmp);
        channel.write(value);
    }

    SSTable(Path file) {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            mmap = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        } catch (IOException e) {
            mmap = null;
            e.printStackTrace();
        }
    }

    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        synchronized (this) {
            return new DiskIterator<Record>(fromKey, toKey, mmap.duplicate().slice().asReadOnlyBuffer());
        }

    }
}
