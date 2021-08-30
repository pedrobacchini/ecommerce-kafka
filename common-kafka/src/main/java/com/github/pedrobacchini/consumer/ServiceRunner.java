package com.github.pedrobacchini.consumer;

import java.util.concurrent.Executors;

public class ServiceRunner<T> {

    private final ServiceProvider<T> serviceProvider;

    public ServiceRunner(ServiceFactory<T> factory) {
        this.serviceProvider = new ServiceProvider<>(factory);
    }

    public void start(int threadCount) {
        var pool = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            pool.submit(serviceProvider);
        }
    }
}
