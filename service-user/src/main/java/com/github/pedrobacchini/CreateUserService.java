package com.github.pedrobacchini;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

public class CreateUserService {

    private final Connection connection;

    CreateUserService() throws SQLException {
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
        var createUserService = new CreateUserService();
        var overrideProperties = Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, CreateUserService.class.getSimpleName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName(),
                GsonDeserializer.TYPE_CONFIG, Order.class.getName()
        );
        try (var service = new KafkaService<>(
                "ECOMMERCE_NEW_ORDER",
                createUserService::parse,
                overrideProperties)) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Order> record) throws SQLException {
        System.out.println("__________________________________");
        System.out.println("Processing new order, checking for new user");
        System.out.println(record.value());
        var order = record.value();
        if(isNewUser(order.getEmail())) {
            insertNewUser(order.getEmail());
        }
    }

    private void insertNewUser(String email) throws SQLException {
        var insert = connection.prepareStatement("insert into Users (uuid, email) values (?,?)");
        insert.setString(1, UUID.randomUUID().toString());
        insert.setString(2, email);
        insert.execute();
        System.out.println("Creating user with email "+email);
    }

    private boolean isNewUser(String email) throws SQLException {
        var exists = connection.prepareStatement("select uuid from Users where email = ? limit 1");
        exists.setString(1, email);
        var results = exists.executeQuery();
        return !results.next();
    }
}
