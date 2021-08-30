package com.github.pedrobacchini;

import com.github.pedrobacchini.dispatcher.KafkaDispatcher;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet implements Servlet {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        orderDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        try {

            // we are not caring about any security issues, we are only showing how to use http as a starting point
            var email = req.getParameter("email");
            var amount = new BigDecimal(req.getParameter("amount"));
            var orderId = req.getParameter("uuid");
            Order order = new Order(orderId, amount, email);

            try(var database = new OrdersDatabase()) {
                if(database.saveNewOrder(order)) {
                    CorrelationId id = new CorrelationId(NewOrderServlet.class.getSimpleName());
                    orderDispatcher.send(
                            "ECOMMERCE_NEW_ORDER",
                            email,
                            id,
                            order
                    );

                    System.out.println("New Order sent successfully.");
                    resp.setStatus(HttpServletResponse.SC_OK);
                    resp.getWriter().println("New order sent");
                } else {
                    System.out.println("Old Order received.");
                    resp.setStatus(HttpServletResponse.SC_OK);
                    resp.getWriter().println("Old order received");
                }
            }

        } catch (ExecutionException | InterruptedException | SQLException e) {
            throw new ServletException();
        }
    }
}
