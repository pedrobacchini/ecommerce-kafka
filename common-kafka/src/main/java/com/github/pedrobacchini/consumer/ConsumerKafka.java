package com.github.pedrobacchini.consumer;

public interface ConsumerKafka<T> {
    void accept(T record) throws Exception;
}
