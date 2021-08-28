package com.github.pedrobacchini;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class BatchSendMessageService {

    private final Connection connection;

    BatchSendMessageService() throws SQLException {
        var url = "jdbc:sqlite:users_database.db";
        this.connection = DriverManager.getConnection(url);
        try {
            connection.createStatement().execute(
                    "create table Users(uuid varchar(200) primary key, email varchar(200))"
            );
        } catch (SQLException ex) {
            //be careful, the sql could be wrong, be realllllly careful
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException {
        var batchSendMessageService = new BatchSendMessageService();
        var overrideProperties = Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, BatchSendMessageService.class.getSimpleName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName()
        );
        try (var service = new KafkaService<>(
                "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS",
                batchSendMessageService::parse,
                overrideProperties)) {
            service.run();
        }
    }

    private final KafkaDispatcher<User> userKafkaDispatcher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Message<String>> record) throws ExecutionException, InterruptedException, SQLException {
        System.out.println("__________________________________");
        System.out.println("Processing new batch");
        var message = record.value();
        System.out.println("Topic: " + message.getPayload());

        for (User user : getAllUsers()) {
            userKafkaDispatcher.send(
                    record.value().getPayload(),
                    user.getUuid(),
                    message.getId().continueWith(BatchSendMessageService.class.getSimpleName()),
                    user
            );
        }
    }

    private List<User> getAllUsers() throws SQLException {
        var resultSet = connection.prepareStatement("select uuid from Users").executeQuery();
        List<User> users = new ArrayList<>();
        while (resultSet.next()) {
            users.add(new User(resultSet.getString(1)));
        }
        return users;
    }
}
