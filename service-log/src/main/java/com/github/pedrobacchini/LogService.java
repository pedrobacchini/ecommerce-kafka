package com.github.pedrobacchini;

import com.github.pedrobacchini.consumer.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class LogService {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var logService = new LogService();
        var overrideProperties = Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, LogService.class.getSimpleName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
        );
        try (var kafkaService = new KafkaService<>(
                Pattern.compile("ECOMMERCE.*"),
                logService::parse,
                overrideProperties)) {
            kafkaService.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<String>> record) {
        System.out.println("__________________________________");
        System.out.println("LOG " + record.topic());
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
    }
}
