package com.github.pedrobacchini;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {

    private final Consumer<ConsumerRecord<String, T>> parse;
    private final KafkaConsumer<String, T> consumer;

    KafkaService(
            String topic,
            Consumer<ConsumerRecord<String, T>> parse,
            Map<String, String> overrideProperties
    ) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(getProperties(overrideProperties));
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(
            Pattern pattern,
            Consumer<ConsumerRecord<String, T>> parse,
            Map<String, String> overrideProperties
    ) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(getProperties(overrideProperties));
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

    private Properties getProperties(Map<String, String> overrideProperties) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,  "1");
        properties.putAll(overrideProperties);
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
