package ru.mail.polis.lsm.saveliyschur.sstservice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Objects;

public class SSTableFileWorker {

    private static final Logger log = LoggerFactory.getLogger(SSTableFileWorker.class);

    public static ISSTable write(Iterator<Record> records, Path file) throws IOException {
        log.info("Write file.");
        Path indexFile = getIndexFile(file);
        Path tmpFileName = getTmpFile(file);
        Path tmpIndexName = getTmpFile(indexFile);

        try (
                FileChannel fileChannel = openForWrite(tmpFileName);
                FileChannel indexChannel = openForWrite(tmpIndexName)
        ) {
            ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            while (records.hasNext()) {
                long position = fileChannel.position();
                if (position > Integer.MAX_VALUE) {
                    throw new IllegalStateException("File is too long");
                }
                writeInt((int) position, indexChannel, size);

                Record record = records.next();
                writeValueWithSize(record.getKey(), fileChannel, size);
                if (record.isTombstone()) {
                    writeInt(-1, fileChannel, size);
                } else {
                    // value is null for tombstones only
                    ByteBuffer value = Objects.requireNonNull(record.getValue());
                    writeValueWithSize(value, fileChannel, size);
                }
            }
            fileChannel.force(false);
        }

        rename(indexFile, tmpIndexName);
        rename(file, tmpFileName);

        return new SSTable(file);
    }

    private static Path resolveWithExt(Path file, String ext) {
        return file.resolveSibling(file.getFileName() + ext);
    }

    private static Path getIndexFile(Path file) {
        return resolveWithExt(file, ".idx");
    }

    private static Path getTmpFile(Path file) {
        return resolveWithExt(file, ".tmp");
    }

    private static FileChannel openForWrite(Path tmpFileName) throws IOException {
        return FileChannel.open(
                tmpFileName,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
        );
    }

    private static void writeValueWithSize(ByteBuffer value, WritableByteChannel channel, ByteBuffer tmp) throws IOException {
        writeInt(value.remaining(), channel, tmp);
        channel.write(tmp);
        channel.write(value);
    }

    private static void writeInt(int value, WritableByteChannel channel, ByteBuffer tmp) throws IOException {
        tmp.position(0);
        tmp.putInt(value);
        tmp.position(0);

        channel.write(tmp);
    }

    private static void rename(Path indexFile, Path tmpIndexName) throws IOException {
        Files.deleteIfExists(indexFile);
        Files.move(tmpIndexName, indexFile, StandardCopyOption.ATOMIC_MOVE);
    }
}
