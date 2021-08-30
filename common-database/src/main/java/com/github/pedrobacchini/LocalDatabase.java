package com.github.pedrobacchini;

import java.sql.*;

public class LocalDatabase {

    private Connection connection;

    public LocalDatabase(String databaseName) throws SQLException {
        var url = "jdbc:sqlite:" + databaseName + ".db";
        this.connection = DriverManager.getConnection(url);
    }

    // yes, this is way too generic according to your database tool, avoid injection
    public void createIfNotExists(String sql) {
        try {
            connection.createStatement().execute(sql);
        } catch (SQLException ex) {
            //be careful, the sql could be wrong, be realllllly careful
            ex.printStackTrace();
        }
    }

    public boolean update(String statement, String ... params) throws SQLException {
        return prepare(statement, params).execute();
    }

    public ResultSet query(String statement, String... params) throws SQLException {
        return prepare(statement, params).executeQuery();
    }

    private PreparedStatement prepare(String statement, String[] params) throws SQLException {
        var preparedStatement = connection.prepareStatement(statement);
        for (int i = 0; i < params.length; i++) {
            preparedStatement.setString(i+1, params[0]);
        }
        return preparedStatement;
    }
}
