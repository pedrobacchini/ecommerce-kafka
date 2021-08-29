package com.github.pedrobacchini;

import com.github.pedrobacchini.consumer.GsonDeserializer;
import com.github.pedrobacchini.consumer.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ReadingReportService {

    private static final Path SOURCE = new File("src/main/resources/report.txt").toPath();

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var reportService = new ReadingReportService();
        var overrideProperties = Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, ReadingReportService.class.getSimpleName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName()
        );
        try (var service = new KafkaService<>(
                "ECOMMERCE_USER_GENERAING_READING_REPORT",
                reportService::parse,
                overrideProperties)) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<User>> record) throws IOException {
        System.out.println("__________________________________");
        User user = record.value().getPayload();
        System.out.println("Processing report for " + user);
        File target = new File(user.getReportPath());
        IO.copyTo(SOURCE, target);
        IO.append(target, "Created for "+user.getUuid());
        System.out.println("File Created "+target.getAbsolutePath());
    }
}
