package com.github.pedrobacchini.consumer;

public interface ServiceFactory<T> {
    ConsumerService<T> create() throws Exception;
}
