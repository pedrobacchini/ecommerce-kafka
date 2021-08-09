package com.github.pedrobacchini;

import java.util.concurrent.ExecutionException;

public interface ConsumerKafka<T> {
    void accept(T record) throws ExecutionException, InterruptedException;
}
