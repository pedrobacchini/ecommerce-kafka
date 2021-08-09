package com.github.pedrobacchini;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.regex.Pattern;

public class LogService {

    public static void main(String[] args) {
        var logService = new LogService();
        var overrideProperties = Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, LogService.class.getSimpleName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                GsonDeserializer.TYPE_CONFIG, String.class.getName()
        );
        try (var kafkaService = new KafkaService<>(
                Pattern.compile("ECOMMERCE.*"),
                logService::parse,
                overrideProperties)) {
            kafkaService.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("__________________________________");
        System.out.println("LOG " + record.topic());
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
    }
}
