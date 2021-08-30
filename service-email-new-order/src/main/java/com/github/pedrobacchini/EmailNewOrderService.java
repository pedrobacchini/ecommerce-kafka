package com.github.pedrobacchini;

import com.github.pedrobacchini.consumer.ConsumerService;
import com.github.pedrobacchini.consumer.GsonDeserializer;
import com.github.pedrobacchini.consumer.ServiceRunner;
import com.github.pedrobacchini.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.ExecutionException;

public class EmailNewOrderService implements ConsumerService<Order> {

    public static void main(String[] args) {
        new ServiceRunner<>(EmailNewOrderService::new).start(1);
    }

    private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();

    @Override
    public String getDeserializerClass() {
        return GsonDeserializer.class.getName();
    }

    @Override
    public String getConsumerGroup() {
        return EmailNewOrderService.class.getSimpleName();
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
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
