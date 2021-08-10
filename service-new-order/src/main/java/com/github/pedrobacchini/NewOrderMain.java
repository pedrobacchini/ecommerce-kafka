package com.github.pedrobacchini;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        var email = Math.random()+"@email.com";

        for (int i = 0; i < 10; i++) {
            var userId = UUID.randomUUID().toString();
            try (var kafkaDispatcher = new KafkaDispatcher<Order>()) {
                var orderId = UUID.randomUUID().toString();
                var amount = BigDecimal.valueOf(Math.random() * 50000 + 1);
                Order order = new Order(userId, orderId, amount, email);
                kafkaDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);
            }

            try (var kafkaDispatcher = new KafkaDispatcher<String>()) {
                var emailCode = "Thank you for your order! We are processing your order";
                kafkaDispatcher.send("ECOMMERCE_SEND_EMAIL", userId, emailCode);
            }
        }
    }
}
