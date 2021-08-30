package com.github.pedrobacchini.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Map;
import java.util.concurrent.Callable;

public class ServiceProvider<T> implements Callable<Void> {

    private final ServiceFactory<T> factory;

    public ServiceProvider(ServiceFactory<T> factory) {
        this.factory = factory;
    }

    @Override
    public Void call() throws Exception {
        var service = factory.create();
        var overrideProperties = Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, service.getConsumerGroup(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, service.getDeserializerClass()
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
