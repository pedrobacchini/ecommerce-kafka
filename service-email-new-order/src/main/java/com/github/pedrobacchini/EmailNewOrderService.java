package com.github.pedrobacchini;

import com.github.pedrobacchini.consumer.GsonDeserializer;
import com.github.pedrobacchini.consumer.KafkaService;
import com.github.pedrobacchini.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class EmailNewOrderService {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var emailService = new EmailNewOrderService();
        var overrideProperties = Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, EmailNewOrderService.class.getSimpleName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName()
        );
        try (var service = new KafkaService<>(
                "ECOMMERCE_NEW_ORDER",
                emailService::parse,
                overrideProperties)) {
            service.run();
        }
    }

    private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        System.out.println("__________________________________");
        System.out.println("Processing new order, preparing email");
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        Message<Order> message = record.value();

        var emailCode = "Thank you for your order! We are processing your order";
        CorrelationId id = message.getId().continueWith(EmailNewOrderService.class.getSimpleName());
        emailDispatcher.send(
                "ECOMMERCE_SEND_EMAIL",
                message.getPayload().getEmail(),
                id,
                emailCode
        );
    }
}
