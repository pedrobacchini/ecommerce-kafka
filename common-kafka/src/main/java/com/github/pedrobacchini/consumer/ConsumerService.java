package com.github.pedrobacchini.consumer;

import com.github.pedrobacchini.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public interface ConsumerService<T> {

    String getDeserializerClass();

    String getConsumerGroup();

    String getTopic();

    void parse(ConsumerRecord<String, Message<T>> record) throws IOException;
}
