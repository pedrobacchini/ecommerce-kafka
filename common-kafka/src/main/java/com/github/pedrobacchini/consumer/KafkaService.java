package com.github.pedrobacchini.consumer;

import com.github.pedrobacchini.Message;
import com.github.pedrobacchini.dispatcher.GsonSerializer;
import com.github.pedrobacchini.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {

    private final ConsumerKafka<ConsumerRecord<String, Message<T>>> parse;
    private final KafkaConsumer<String, Message<T>> consumer;

    public KafkaService(
            String topic,
            ConsumerKafka<ConsumerRecord<String, Message<T>>> parse,
            Map<String, String> overrideProperties
    ) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(getProperties(overrideProperties));
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(
            Pattern pattern,
            ConsumerKafka<ConsumerRecord<String, Message<T>>> parse,
            Map<String, String> overrideProperties
    ) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(getProperties(overrideProperties));
        this.consumer.subscribe(pattern);
    }

    public void run() throws ExecutionException, InterruptedException {
        try(var deadLetter = new KafkaDispatcher<>()) {
            while (true) {
                var records = consumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty()) {
                    System.out.println("Encontrei registros " + records.count());
                    for (var record : records) {
                        try {
                            parse.accept(record);
                        } catch (Exception e) {
                            var message = record.value();
                            deadLetter.send("ECOMMERCE_DEADLETTER", String.valueOf(message.getId()),
                                    message.getId().continueWith("DeadLetter"),
                                    new GsonSerializer<>().serialize("", message.getPayload()));
                        }
                    }
                }
            }
        }
    }

    private Properties getProperties(Map<String, String> overrideProperties) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,  "1");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.putAll(overrideProperties);
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
