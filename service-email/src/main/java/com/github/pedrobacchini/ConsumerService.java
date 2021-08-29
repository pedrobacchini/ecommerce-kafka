package com.github.pedrobacchini;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerService<T> {

    String getConsumerGroup();

    String getTopic();

    void parse(ConsumerRecord<String, Message<T>> record);
}
