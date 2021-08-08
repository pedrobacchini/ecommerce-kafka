package com.github.pedrobacchini;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        try(KafkaDispatcher kafkaDispatcher = new KafkaDispatcher()) {
            var key = UUID.randomUUID().toString();
            var value = key + ",100";
            kafkaDispatcher.send("ECOMMERCE_NEW_ORDER", key, value);
            var email = "Thank you for your order! We are processing your order";
            kafkaDispatcher.send("ECOMMERCE_SEND_EMAIL", email, email);
        }
    }
}
