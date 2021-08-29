package com.github.pedrobacchini;

import com.github.pedrobacchini.consumer.ConsumerService;
import com.github.pedrobacchini.consumer.GsonDeserializer;
import com.github.pedrobacchini.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public class ReadingReportService implements ConsumerService<User> {

    private static final Path SOURCE = new File("src/main/resources/report.txt").toPath();

    public static void main(String[] args) {
        new ServiceRunner<>(ReadingReportService::new).start(5);
    }

    @Override
    public String getDeserializerClass() {
        return GsonDeserializer.class.getName();
    }

    @Override
    public String getConsumerGroup() {
        return ReadingReportService.class.getSimpleName();
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_USER_GENERAING_READING_REPORT";
    }

    @Override
    public void parse(ConsumerRecord<String, Message<User>> record) throws IOException {
        System.out.println("__________________________________");
        User user = record.value().getPayload();
        System.out.println("Processing report for " + user);
        File target = new File(user.getReportPath());
        IO.copyTo(SOURCE, target);
        IO.append(target, "Created for "+user.getUuid());
        System.out.println("File Created "+target.getAbsolutePath());
    }
}
