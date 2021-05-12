package ru.mail.polis.lsm.gromov_maxim;

import java.io.IOException;

public class Main {
    private void test() throws IOException, InterruptedException {
        System.out.println("заглушка");
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final Benchmark benchmark = new Benchmark();
        benchmark.printResult();
    }
}
