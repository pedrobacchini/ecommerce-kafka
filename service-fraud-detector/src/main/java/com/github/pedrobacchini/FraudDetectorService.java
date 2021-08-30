package com.github.pedrobacchini;

import com.github.pedrobacchini.consumer.ConsumerService;
import com.github.pedrobacchini.consumer.GsonDeserializer;
import com.github.pedrobacchini.consumer.ServiceRunner;
import com.github.pedrobacchini.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService implements ConsumerService<Order> {

    private final LocalDatabase localDatabase;

    public FraudDetectorService() throws SQLException {
        this.localDatabase = new LocalDatabase("frauds_database");
        this.localDatabase.createIfNotExists("create table Orders(uuid varchar(200) primary key, is_fraud boolean)");
    }

    public static void main(String[] args) {
        new ServiceRunner<>(FraudDetectorService::new).start(1);
    }

    private final KafkaDispatcher<Order> orderKafkaDispatcher = new KafkaDispatcher<>();

    @Override
    public String getDeserializerClass() {
        return GsonDeserializer.class.getName();
    }

    @Override
    public String getConsumerGroup() {
        return FraudDetectorService.class.getSimpleName();
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException, SQLException {
        System.out.println("__________________________________");
        System.out.println("Processing new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        var message = record.value();
        var order = message.getPayload();

        if (wasProcesses(order)) {
            System.out.println("Order " + order.getOrderId() + "was already processed");
            return;
        }

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (isFraud(order)) {
            localDatabase.update("insert into Orders(uuid, is_fraud) values (?, true)", order.getOrderId());
            System.out.println("Order is a fraud!!!!!");
            orderKafkaDispatcher.send(
                    "ECOMMERCE_ORDER_REJECTED",
                    order.getEmail(),
                    message.getId().continueWith(FraudDetectorService.class.getSimpleName()),
                    order
            );
        } else {
            localDatabase.update("insert into Orders(uuid, is_fraud) values (?, false)", order.getOrderId());
            System.out.println("Approved: " + order);
            orderKafkaDispatcher.send(
                    "ECOMMERCE_ORDER_APPROVED",
                    order.getEmail(),
                    message.getId().continueWith(FraudDetectorService.class.getSimpleName()),
                    order
            );
        }
    }

    private boolean wasProcesses(Order order) throws SQLException {
        var results = localDatabase.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId());
        return results.next();
    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("45000")) >= 0;
    }
}
