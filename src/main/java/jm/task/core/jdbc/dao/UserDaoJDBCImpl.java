package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {
    private static final Connection connection = Util.getConnection();

    public void createUsersTable() {
        String createTable = "CREATE TABLE IF NOT EXISTS user" +
                "(id BIGINT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(45), " +
                "lastName VARCHAR(45), age TINYINT)";
        try (connection) {
            connection.setAutoCommit(false);

            PreparedStatement preparedStatement = connection.prepareStatement(createTable);
            preparedStatement.executeUpdate();

            connection.commit();
        } catch (SQLException e) {
            System.out.println("Table is`s created");
        }
    }
    public void dropUsersTable()  {
        try (connection) {
            connection.setAutoCommit(false);

            Statement statement = connection.createStatement();
            statement.executeUpdate("DROP TABLE IF EXISTS user");

            connection.commit();
        } catch (SQLException e) {
            System.out.println("Can`t delete table");
        }
    }
    public void saveUser(String name, String lastName, byte age) {
        try {
            connection.setAutoCommit(false);

            PreparedStatement preparedStatement = connection.prepareStatement(
                    "INSERT INTO user (name, lastName,age)" + "VALUES (?,?,?)");
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setByte(3, age);
            preparedStatement.executeUpdate();

            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException e1) {
                System.out.println("User is not saved");
            }
        }
    }

    public void removeUserById(long id) {

        try (connection) {
            connection.setAutoCommit(false);

            PreparedStatement preparedStatement = connection.prepareStatement(
                    "DELETE FROM user WHERE id = ?");
            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();

            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException e1) {
                System.out.println("User is not removed");
            }
        }
    }

    public List<User> getAllUsers() {
        List<User> userList = new ArrayList<>();
        try (connection) {
            ResultSet resultSet = connection.createStatement().executeQuery
                    ("SELECT * FROM user");
            while (resultSet.next()) {
                long id = resultSet.getLong("id");
                String name = resultSet.getString("name");
                String lastName = resultSet.getString("lastName");
                byte age = resultSet.getByte("age");
                User user = new User(id, name, lastName, age);
                userList.add(user);
            }
        } catch (SQLException e) {
            System.out.println("Ошибка");
        }
        return userList;
    }

    public void cleanUsersTable()  {
        try (connection) {
            connection.setAutoCommit(false);

            Statement statement = connection.createStatement();
            statement.executeUpdate("TRUNCATE TABLE user");

            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                System.out.println("Table not cleaned");
            }
        }
    }
}

