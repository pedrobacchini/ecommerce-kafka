package com.github.pedrobacchini.consumer;

import com.github.pedrobacchini.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerService<T> {

    String getDeserializerClass();

    String getConsumerGroup();

    String getTopic();

    // you may argue that a ConsumerException would be better
    // and its ok, it can be better
    void parse(ConsumerRecord<String, Message<T>> record) throws Exception;
}
