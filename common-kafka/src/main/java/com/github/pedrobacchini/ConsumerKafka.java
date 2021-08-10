package com.github.pedrobacchini;

public interface ConsumerKafka<T> {
    void accept(T record) throws Exception;
}
