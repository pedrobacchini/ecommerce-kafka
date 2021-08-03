package com.github.pedrobacchini;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public class KafkaService {

    private final Consumer<ConsumerRecord<String, String>> parse;
    private final KafkaConsumer<String, String> consumer;

    KafkaService(String topic, String groupId, Consumer<ConsumerRecord<String, String>> parse) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(properties(groupId));
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(Pattern pattern, String groupId, Consumer<ConsumerRecord<String, String>> parse) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(properties(groupId));
        this.consumer.subscribe(pattern);
    }

    void run() {
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("Encontrei registros " + records.count());
                for (var record : records) {
                    parse.accept(record);
                }
            }
        }
    }

    private static Properties properties(String groupId) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,  "1");
        return properties;
    }
}
