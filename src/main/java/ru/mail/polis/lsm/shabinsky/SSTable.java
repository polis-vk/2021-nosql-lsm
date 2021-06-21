package ru.mail.polis.lsm.shabinsky;

import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class SSTable {

    public static final String NAME = "sstable_";
    public static final String COMP = "comp_";
    public static final String IDX = "_idx";
    public static final String SAVE = ".save";
    public static final String TEMP = ".temp";

    private static Method CLEAN;

    static {
        try {
            Class<?> classs = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = classs.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            e.printStackTrace();
        }
    }

    private final MappedByteBuffer mmapRec;
    private final MappedByteBuffer mmapOffsets;
    private final Integer count;

    /**
     * Load from Dir.
     *
     * @param dir Path
     * @return List
     * @throws IOException exception
     */
    public static List<SSTable> loadFromDir(Path dir) throws IOException {
        List<SSTable> tables = new ArrayList<>();

        for (int i = 0; ; i++) {
            Path saveFileName = dir.resolve(sstableName(i) + SAVE);
            Path tmpFileName = dir.resolve(sstableName(i) + TEMP);

            if (!Files.exists(saveFileName)) {
                if (Files.exists(tmpFileName)) {
                    Files.move(tmpFileName, saveFileName, StandardCopyOption.ATOMIC_MOVE);
                } else {
                    break;
                }
            }

            SSTable ssTable = new SSTable(dir, sstableName(i));
            tables.add(ssTable);
        }

        return tables;
    }

    /**
     * Write.
     *
     * @param records  Records
     * @param path     Path
     * @param fileName String
     * @return SSTable
     * @throws IOException exception
     */
    public static SSTable write(PeekingIterator records, Path path, String fileName, final int MEM_LIM) throws IOException {
        Path saveFileName = path.resolve(fileName + SAVE);
        Path tmpFileName = path.resolve(fileName + TEMP);
        Path saveIdxFileName = path.resolve(fileName + IDX + SAVE);
        Path tmpIdxFileName = path.resolve(fileName + IDX + TEMP);

        try (
            FileChannel fileChannel = FileChannel.open(
                tmpFileName,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);

            FileChannel indexChannel = FileChannel.open(
                tmpIdxFileName,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)
        ) {

            ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            int offset = 0;
            int countOffset = 0;
            int mem = 0;

            while (records.hasNext()) {
                // records
                Record record = records.peek();

                mem += record.sizeOf();
                if (mem >= MEM_LIM) break;
                records.next();

                writeInt(record.getKey(), fileChannel, size);
                writeInt(record.getValue(), fileChannel, size);

                // index
                ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES).putInt(offset);
                buffer.position(0);
                indexChannel.write(buffer);
                countOffset++;

                offset +=
                    Integer.BYTES + record.getKey().remaining() + Integer.BYTES;

                if (!record.isTombstone()) offset += record.getValue().remaining();
            }

            // index
            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES).putInt(countOffset);
            buffer.position(0);
            indexChannel.write(buffer);

            fileChannel.force(false);
            indexChannel.force(false);
        }
        Files.deleteIfExists(saveFileName);
        Files.move(tmpFileName, saveFileName, StandardCopyOption.ATOMIC_MOVE);
        Files.deleteIfExists(saveIdxFileName);
        Files.move(tmpIdxFileName, saveIdxFileName, StandardCopyOption.ATOMIC_MOVE);
        return new SSTable(path, fileName);
    }

    private static void
    writeInt(@Nullable ByteBuffer value, WritableByteChannel channel, ByteBuffer tmp) throws IOException {
        if (value == null) {
            tmp.position(0);
            tmp.putInt(0);
            tmp.position(0);
            channel.write(tmp);
        } else {
            tmp.position(0);
            tmp.putInt(value.remaining());
            tmp.position(0);
            channel.write(tmp);
            channel.write(value);
        }
    }

    private static boolean cleanComp(Path dir, String nameSSTable, String nameComp) throws IOException {
        Path saveFileName = dir.resolve(nameSSTable + SAVE);
        Path tmpFileName = dir.resolve(nameSSTable + TEMP);

        if (!Files.exists(saveFileName)) {
            if (Files.exists(tmpFileName)) {
                Files.move(tmpFileName, saveFileName, StandardCopyOption.ATOMIC_MOVE);
            } else {
                return false;
            }
        }
        Files.delete(saveFileName);

        Path saveSS = dir.resolve(nameComp + SAVE);
        Path tmpSS = dir.resolve(nameComp + TEMP);

        if (Files.exists(saveSS)) {
            Files.move(saveSS, saveFileName, StandardCopyOption.ATOMIC_MOVE);
        } else {
            if (Files.exists(tmpSS)) {
                Files.move(tmpSS, saveSS, StandardCopyOption.ATOMIC_MOVE);
                Files.move(saveSS, saveFileName, StandardCopyOption.ATOMIC_MOVE);
            }
        }
        return true;
    }

    /**
     * CleanForComp.
     *
     * @param dir Path
     * @throws IOException exception
     */
    public static void cleanForComp(Path dir) throws IOException {
        for (int i = 0; ; i++) {
            if (!cleanComp(dir, sstableName(i), sstableCompName(i))) break;
            if (!cleanComp(dir, sstableName(i) + IDX, sstableCompName(i) + IDX)) break;
        }
    }

    public static String sstableName(int number) {
        return NAME + number;
    }

    public static String sstableCompName(int number) {
        return NAME + COMP + number;
    }

    /**
     * SSTable.
     *
     * @param path     Path
     * @param fileName String
     * @throws IOException exception
     */
    public SSTable(Path path, String fileName) throws IOException {
        Path saveFileName = path.resolve(fileName + SAVE);
        Path tmpFileName = path.resolve(fileName + TEMP);

        if (!Files.exists(saveFileName)) {
            if (Files.exists(tmpFileName)) {
                Files.move(tmpFileName, saveFileName, StandardCopyOption.ATOMIC_MOVE);
            } else {
                mmapRec = null;
                mmapOffsets = null;
                count = 0;
                return;
            }
        }

        try (FileChannel channel = FileChannel.open(saveFileName, StandardOpenOption.READ)) {
            mmapRec = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        }

        Path saveIdxFileName = path.resolve(fileName + IDX + SAVE);
        Path tmpIdxFileName = path.resolve(fileName + IDX + TEMP);

        if (!Files.exists(saveIdxFileName)) {
            if (Files.exists(tmpIdxFileName)) {
                Files.move(tmpIdxFileName, saveIdxFileName, StandardCopyOption.ATOMIC_MOVE);
            } else {
                mmapOffsets = null;
                count = 0;
                return;
            }
        }

        try (FileChannel channel = FileChannel.open(saveIdxFileName, StandardOpenOption.READ)) {
            mmapOffsets = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
            this.count = mmapOffsets.getInt((int) (channel.size() - Integer.BYTES));
            mmapOffsets.position(0);
        }
    }

    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return new Iterator<>() {
            private int currentPos = findPos(fromKey, true);
            private final int endPos = findPos(toKey, false);

            @Override
            public boolean hasNext() {
                return currentPos < endPos;
            }

            @Override
            public Record next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("No elements");
                }
                Record record = getRecord(currentPos);
                currentPos++;
                return record;
            }
        };
    }

    private Record getRecord(int pos) {
        mmapOffsets.position(0);
        mmapRec.position(0);

        int offset = mmapOffsets.position(pos * Integer.BYTES).getInt();

        mmapRec.position(offset);
        int keySize = mmapRec.getInt();
        ByteBuffer key = mmapRec.slice().limit(keySize).asReadOnlyBuffer();

        mmapRec.position(mmapRec.position() + keySize);

        int valueSize = mmapRec.getInt();
        ByteBuffer value = null;
        if (valueSize != 0) value = mmapRec.slice().limit(valueSize).asReadOnlyBuffer();

        mmapOffsets.position(0);
        mmapRec.position(0);

        if (value == null) {
            return Record.tombstone(key);
        } else {
            return Record.of(key, value);
        }
    }

    private int findPos(@Nullable ByteBuffer key, boolean isStart) {
        if (key == null && isStart) return 0;
        if (key == null) return this.count;

        int left = 0;
        int right = this.count;

        while (left < right) {
            int mid = left + ((right - left) >>> 1);

            ByteBuffer keyMid = getRecord(mid).getKey();
            int compare = keyMid.compareTo(key);

            if (compare < 0) {
                left = mid + 1;
            } else if (compare > 0) {
                right = mid;
            } else {
                return mid;
            }
        }

        return left;
    }

    /**
     * Close.
     *
     * @throws IOException exception
     */
    public void close() throws IOException {
        IOException exception = null;
        try {
            free(mmapRec);
        } catch (IOException e) {
            exception = e;
        }

        try {
            free(mmapOffsets);
        } catch (IOException e) {
            if (exception == null) {
                exception = e;
            } else {
                exception.addSuppressed(e);
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    private static void free(MappedByteBuffer buffer) throws IOException {
        try {
            CLEAN.invoke(null, buffer);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IOException(e);
        }
    }
}
