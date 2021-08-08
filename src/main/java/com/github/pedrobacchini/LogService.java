package com.github.pedrobacchini;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.regex.Pattern;

public class LogService {

    public static void main(String[] args) {
        var logService = new LogService();
        var kafkaService = new KafkaService<String>(
                Pattern.compile("ECOMMERCE.*"),
                LogService.class.getSimpleName(),
                logService::parse,
                String.class);
        kafkaService.run();
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
