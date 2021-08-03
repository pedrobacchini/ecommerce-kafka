package com.github.pedrobacchini;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {

    public static void main(String[] args) {
        var emailService = new EmailService();
        var service = new KafkaService(
                "ECOMMERCE_SEND_EMAIL",
                EmailService.class.getSimpleName(),
                emailService::parse
        );
        service.run();
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("__________________________________");
        System.out.println("Send email");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Email sent");
    }
}
