package ru.mail.polis.lsm.gromov_maxim;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Benchmark {
    private final DAO dao;
    private final List<Long> time;
    private final int testingCount = 100;

    public Benchmark() {
        dao = new InMemoryDAO();
        time = new CopyOnWriteArrayList<>();
    }

    public void printResult() throws IOException, InterruptedException {
        int thread_size = 4;
        ExecutorService executorService = Executors.newFixedThreadPool(thread_size);
        run(executorService, thread_size);
        Thread.sleep(1000);
        System.out.println("===4 threads===");
        System.out.println("Average time = " + getSum() / testingCount);
        time.clear();
        dao.close();
        executorService.shutdown();
        if (!executorService.awaitTermination(20, TimeUnit.SECONDS)) {
            throw new InterruptedException();
        }

        thread_size *= 2; //8
        executorService = Executors.newFixedThreadPool(thread_size);
        run(executorService, thread_size);
        Thread.sleep(1000);
        System.out.println("===8 threads===");
        System.out.println("Average time = " + getSum() / testingCount);
        time.clear();
        dao.close();
        executorService.shutdown();
        if (!executorService.awaitTermination(20, TimeUnit.SECONDS)) {
            throw new InterruptedException();
        }

        thread_size *= 2; //16
        executorService = Executors.newFixedThreadPool(thread_size);
        run(executorService, thread_size);
        Thread.sleep(1000);
        System.out.println("===16 threads===");
        System.out.println("Average time = " + getSum() / testingCount);
        time.clear();
        dao.close();
        executorService.shutdown();
        if (!executorService.awaitTermination(20, TimeUnit.SECONDS)) {
            throw new InterruptedException();
        }

        thread_size *= 2; //32
        executorService = Executors.newFixedThreadPool(thread_size);
        run(executorService, thread_size);
        Thread.sleep(1000);
        System.out.println("===32 threads===");
        System.out.println("Average time = " + getSum() / testingCount);
        time.clear();
        dao.close();
        executorService.shutdown();
        if (!executorService.awaitTermination(20, TimeUnit.SECONDS)) {
            throw new InterruptedException();
        }

        thread_size *= 2; //64
        executorService = Executors.newFixedThreadPool(thread_size);
        run(executorService, thread_size);
        Thread.sleep(1000);
        System.out.println("===64 threads===");
        System.out.println("Average time = " + getSum() / testingCount);
        time.clear();
        dao.close();
        executorService.shutdown();
        if (!executorService.awaitTermination(20, TimeUnit.SECONDS)) {
            throw new InterruptedException();
        }
    }

    private void run(ExecutorService executorService, int thread_size) {
        for (int i = 0; i < thread_size; ++i) {
            executorService.execute(() -> {
                long start = System.nanoTime();
                for(int j = 0; j < testingCount; ++j) {
                    dao.upsert(getRandomRecord());
                    dao.range(null, null);
                }
                time.add(System.nanoTime() - start);
            });
        }
    }

    private Record getRandomRecord() {
        final int string_size = 20;
        String symbols = "abcdefghijklmnopqrstuvwxyz";
        String random = new Random().ints(string_size, 0, symbols.length())
                .mapToObj(symbols::charAt)
                .map(Object::toString)
                .collect(Collectors.joining());
        ByteBuffer key = ByteBuffer.wrap((random).getBytes(StandardCharsets.UTF_8));
        ByteBuffer value = ByteBuffer.wrap((random).getBytes(StandardCharsets.UTF_8));
        return Record.of(key, value);

    }

    private Long getSum() {
        long sum = 0;
        for(var item : time) {
            sum += item;
        }
        return sum;
    }
}
