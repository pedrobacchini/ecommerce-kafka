package com.github.pedrobacchini;

import com.github.pedrobacchini.dispatcher.KafkaDispatcher;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        for (int i = 0; i < 10; i++) {
            try (var kafkaDispatcher = new KafkaDispatcher<Order>()) {
                var orderId = UUID.randomUUID().toString();
                var amount = BigDecimal.valueOf(Math.random() * 50000 + 1);
                var email = Math.random()+"@email.com";
                var id = new CorrelationId(NewOrderMain.class.getSimpleName());
                Order order = new Order(orderId, amount, email);
                kafkaDispatcher.send(
                        "ECOMMERCE_NEW_ORDER",
                        email,
                        id,
                        order
                );
            }
        }
    }
}
