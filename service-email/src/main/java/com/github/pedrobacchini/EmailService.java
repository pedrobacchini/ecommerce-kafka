package com.github.pedrobacchini;

import com.github.pedrobacchini.consumer.ConsumerService;
import com.github.pedrobacchini.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

public class EmailService implements ConsumerService<String> {

    public static void main(String[] args) {
        new ServiceRunner<>(EmailService::new).start(5);
    }

    @Override
    public String getDeserializerClass() {
        return StringDeserializer.class.getName();
    }

    @Override
    public String getConsumerGroup() {
        return EmailService.class.getSimpleName();
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_SEND_EMAIL";
    }

    @Override
    public void parse(ConsumerRecord<String, Message<String>> record) {
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
