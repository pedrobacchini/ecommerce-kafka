package com.github.pedrobacchini;

import java.util.concurrent.Executors;
import java.util.function.Supplier;

public class ServiceRunner<T> {

    private final ServiceProvider<T> serviceProvider;

    public ServiceRunner(Supplier<ConsumerService<T>> factory) {
        this.serviceProvider = new ServiceProvider<>(factory);
    }

    public void start(int threadCount) {
        var pool = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            pool.submit(serviceProvider);
        }
    }
}
