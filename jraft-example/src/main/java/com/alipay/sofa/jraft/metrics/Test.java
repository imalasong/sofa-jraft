package com.alipay.sofa.jraft.metrics;

import com.codahale.metrics.*;
//import com.codahale.metrics.jmx.JmxReporter;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @BelongsProject: sofa-jraft
 * @Author: xiaochangbai
 * @CreateTime: 2024-09-23 18:05
 * @Description: TODO
 * @Version: 1.0
 */
public class Test {

    private static final MetricRegistry metrics = new MetricRegistry();
    private static final Meter requests = metrics.meter("requests");
    private static final Meter requests2 = metrics.meter("requests2");

    private static final Histogram responseSizes = metrics.histogram(MetricRegistry.name(Test.class, "response-sizes"));

    public static void QueueManager(MetricRegistry metrics, String name) {
        Queue queue = new ArrayDeque();
        metrics.register(MetricRegistry.name(Test.class, name, "size"),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return queue.size();
                    }
                });
        new Thread(() -> {
            while (true) {
                queue.add(1L);
                try {
                    Thread.sleep(1000 * 1L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();

    }


    private static final Timer responses = metrics.timer(MetricRegistry.name(Test.class, "responses"));

    public static String handleRequest2() {
        try (final Timer.Context context = responses.time()) {
            // etc;
            try {
                Thread.sleep(new Random().nextInt(100) * 100L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return "ok";
        } // catch and final logic goes here

    }

    public static void main(String[] args) throws IOException {

        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(2, TimeUnit.SECONDS);


//        final JmxReporter jvmReporter = JmxReporter.forRegistry(metrics).build();
//        jvmReporter.start();

        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                while (true) {
                    handleRequest();
                }
            }).start();
        }

        new Thread(() -> {
            while (true) {
                requests2.mark();
                responseSizes.update(1 * 4);
                handleRequest2();
                try {
                    Thread.sleep(1000 * 1L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();

        QueueManager(metrics, "gauges-test");
        System.in.read();
    }

    public static void handleRequest() {
        requests.mark();
        // etc
    }

}
