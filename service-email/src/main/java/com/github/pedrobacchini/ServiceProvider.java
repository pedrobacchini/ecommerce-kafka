package com.github.pedrobacchini;

import com.github.pedrobacchini.consumer.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public class ServiceProvider<T> implements Callable<Void> {

    private final Supplier<ConsumerService<T>> factory;

    public ServiceProvider(Supplier<ConsumerService<T>> factory) {
        this.factory = factory;
    }

    @Override
    public Void call() throws ExecutionException, InterruptedException {
        var service = factory.get();
        var overrideProperties = Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, service.getConsumerGroup(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
        );
        try (var kafkaService = new KafkaService<>(
                service.getTopic(),
                service::parse,
                overrideProperties)) {
            kafkaService.run();
        }
        return null;
    }
}
