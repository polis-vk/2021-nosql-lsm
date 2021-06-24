package ru.mail.polis.lsm.saveliyschur.sstservice;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class SSTable implements Closeable, Comparable {

    private static final Logger log = Logger.getLogger(SSTable.class.getName());
    private static final Method CLEAN;
    private MappedByteBuffer mapp;

    public static final String NAME = "sstable_";
    public static final String EXTENSION = ".sst";

    private final Path path;

    static {
        try {
            Class<?> aClass = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = aClass.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public SSTable(Path path) {
        this.path = path;
    }

    public Path getPath() {
        return path;
    }

    @Override
    public void close() throws IOException {
        IOException exception = null;
        try {
            free(mapp);
        } catch (Throwable t) {
            exception = new IOException(t);
        }

        if (exception != null) {
            throw exception;
        }
        log.info("Close is OK!");
    }

    public MappedByteBuffer getMapp() {
        return mapp;
    }

    public void setMapp(MappedByteBuffer mapp) {
        this.mapp = mapp;
    }

    private static void free(MappedByteBuffer buffer) throws IOException {
        try {
            CLEAN.invoke(null, buffer);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IOException(e);
        }
    }


    @Override
    public int compareTo(Object o) {
        SSTable ssTable = (SSTable) o;
        return this.path.toString().compareTo(ssTable.getPath().toString());
    }
}
