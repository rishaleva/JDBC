package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {

    private final String sqlCreateTable = "CREATE TABLE IF NOT EXISTS user" +
            "(id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL, name VARCHAR(45), " +
            "lastName VARCHAR(45), age TINYINT)";
    private final String sqlDropTable = "DROP TABLE IF EXISTS user";
    private final String sqlSaveUser = "INSERT INTO user (name, lastName,age)" + "VALUES (?,?,?)";
    private final String sqlDeleteById = "DELETE FROM user WHERE id = ?";
    private final String sqlSelectAll = "SELECT * FROM user";
    private final String sqlCleanTable = "TRUNCATE TABLE user";

    public void createUsersTable() {
        try (Connection connection = Util.getConnection()) {
            connection.setAutoCommit(false);

            PreparedStatement preparedStatement = connection.prepareStatement(sqlCreateTable);
            preparedStatement.executeUpdate();

            connection.commit();
            System.out.println("Table was created");
        } catch (SQLException e) {
            System.out.println("Table was NOT created");
        }
    }

    public void dropUsersTable() {
        try (Connection connection = Util.getConnection()) {
            connection.setAutoCommit(false);

            PreparedStatement preparedStatement = connection.prepareStatement(sqlDropTable);
            preparedStatement.executeUpdate();

            connection.commit();
            System.out.println("Table was dropped");
        } catch (SQLException e) {
            System.out.println("Table was NOT dropped");
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        Connection connection = Util.getConnection();
        try (connection) {
            connection.setAutoCommit(false);

            PreparedStatement preparedStatement = connection.prepareStatement(sqlSaveUser);
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setByte(3, age);
            preparedStatement.executeUpdate();

            connection.commit();
            System.out.println("User : " + name + " " + lastName + " " + age + " was saved");
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                System.out.println("User was NOT saved");
            }
        }
    }

    public void removeUserById(long id) {
        Connection connection = Util.getConnection();
        try (connection) {
            connection.setAutoCommit(false);

            PreparedStatement preparedStatement = connection.prepareStatement(sqlDeleteById);
            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();

            connection.commit();
            System.out.println("User with ID " + id + " was deleted ");
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                System.out.println("User with ID was NOT deleted");
            }
        }
    }

    public List<User> getAllUsers() {
        List<User> userList = new ArrayList<>();
        try (Connection connection = Util.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sqlSelectAll)) {
            ResultSet resultSet = preparedStatement.executeQuery(sqlSelectAll);

            while (resultSet.next()) {
                User user = new User();
                user.setId(resultSet.getLong(1));
                user.setName(resultSet.getString(2));
                user.setLastName(resultSet.getString(3));
                user.setAge(resultSet.getByte(4));

                userList.add(user);
                System.out.println("Table was printed");
            }
        } catch (SQLException e) {
            System.out.println("Table was NOT printed");
        }
        return userList;
    }

    public void cleanUsersTable() {
        Connection connection = Util.getConnection();
        try (connection) {
            connection.setAutoCommit(false);

            PreparedStatement preparedStatement = connection.prepareStatement(sqlCleanTable);
            preparedStatement.executeUpdate();

            connection.commit();
            System.out.println("All users was deleted from database");
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                System.out.println("users was NOT deleted from db");
            }
        }
    }
}