package jm.task.core.jdbc.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Util {
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/rishalevabd";
    private static final String DB_USERNAME = "rishaleva";
    private static final String DB_PASSWORD = "root";

    public static Connection getConnection() {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD);
            System.out.println("Conn OK");
        } catch (SQLException e) {
            System.out.println("Conn is not exist");
        }
        return connection;
    }
}
