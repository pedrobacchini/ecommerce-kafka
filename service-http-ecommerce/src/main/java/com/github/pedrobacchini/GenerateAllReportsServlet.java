package com.github.pedrobacchini;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class GenerateAllReportsServlet extends HttpServlet implements Servlet {

    private final KafkaDispatcher<String> batchDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        batchDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        try {

            batchDispatcher.send(
                    "SEND_MESSAGE_TO_ALL_USERS",
                    "USER_GENERAING_READING_REPORT",
                    "USER_GENERAING_READING_REPORT"
            );

            System.out.println("Sent generate report to all users");

            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("Report requests generated");

        } catch (ExecutionException | InterruptedException e) {
            throw new ServletException();
        }
    }
}
