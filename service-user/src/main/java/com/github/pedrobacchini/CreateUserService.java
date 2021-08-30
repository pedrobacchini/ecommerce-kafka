package com.github.pedrobacchini;

import com.github.pedrobacchini.consumer.ConsumerService;
import com.github.pedrobacchini.consumer.GsonDeserializer;
import com.github.pedrobacchini.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

public class CreateUserService implements ConsumerService<Order> {

    private final LocalDatabase localDatabase;

    public CreateUserService() throws SQLException {
        this.localDatabase = new LocalDatabase("users_database");
        this.localDatabase.createIfNotExists("create table Users(uuid varchar(200) primary key, email varchar(200))");
    }

    public static void main(String[] args) {
        new ServiceRunner<>(CreateUserService::new).start(1);
    }

    @Override
    public String getDeserializerClass() {
        return GsonDeserializer.class.getName();
    }

    @Override
    public String getConsumerGroup() {
        return CreateUserService.class.getSimpleName();
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws SQLException {
        System.out.println("__________________________________");
        System.out.println("Processing new order, checking for new user");
        System.out.println(record.value());
        var order = record.value().getPayload();
        if (isNewUser(order.getEmail())) {
            insertNewUser(order.getEmail());
        }
    }

    private void insertNewUser(String email) throws SQLException {
        String uuid = UUID.randomUUID().toString();
        localDatabase.update("insert into Users (uuid, email) values (?,?)", uuid, email);
        System.out.println("Creating user with email " + email);
    }

    private boolean isNewUser(String email) throws SQLException {
        ResultSet results = localDatabase.query("select uuid from Users where email = ? limit 1", email);
        return !results.next();
    }
}
