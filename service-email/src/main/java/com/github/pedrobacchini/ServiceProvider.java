package com.github.pedrobacchini;

import com.github.pedrobacchini.consumer.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public class ServiceProvider {

    public <T> void run(Supplier<ConsumerService<T>> factory) throws ExecutionException, InterruptedException {
        var emailService = factory.get();
        var overrideProperties = Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, emailService.getConsumerGroup(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
        );
        try (var service = new KafkaService<>(
                emailService.getTopic(),
                emailService::parse,
                overrideProperties)) {
            service.run();
        }
    }
}
