package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {

    private static final Logger logger = LogManager.getLogger(UserDaoJDBCImpl.class);
    private final Connection connection = Util.getConnection();
    private final String sqlCreateTable = "CREATE TABLE IF NOT EXISTS \"user\" " + "(id BIGSERIAL PRIMARY KEY, name VARCHAR(45), " + "lastName VARCHAR(45), age SMALLINT)";
    private final String sqlSaveUser = "INSERT INTO \"user\" (name, lastName,age)" + "VALUES (?,?,?)";
    private final String sqlDeleteById = "DELETE FROM \"user\" WHERE id = ?";
    private final String sqlSelectAll = "SELECT * FROM \"user\"";
    private final String sqlCleanTable = "TRUNCATE TABLE \"user\"";
    private final String sqlDropTable = "DROP TABLE IF EXISTS \"user\"";

    public void createUsersTable() {
        try (Connection connection1 = Util.getConnection();
             PreparedStatement preparedStatement = connection1.prepareStatement(sqlCreateTable)) {

            preparedStatement.executeUpdate();
            logger.info("Table was created");
        } catch (SQLException e) {
            logger.warn("Table was NOT created", e);
        }
    }

    public void dropUsersTable() {
        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlDropTable)) {
            connection.setAutoCommit(false);
            preparedStatement.executeUpdate();
            connection.commit();
            logger.info("Table was dropped");
        } catch (SQLException e) {
            logger.warn("Table was NOT dropped", e);
        }
    }

    public void saveUser(String name, String lastName, byte age) {

        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlSaveUser)) {
            connection.setAutoCommit(false);
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
                logger.warn("User was NOT saved", e);
            }
        }
    }

    public void removeUserById(long id) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlDeleteById)) {
            connection.setAutoCommit(false);

            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();

            connection.commit();
            System.out.println("User with ID " + id + " was deleted ");
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                logger.warn("User with ID was NOT deleted", e);
            }
        }
    }

    public List<User> getAllUsers() {
        List<User> userList = new ArrayList<>();
        try (Connection connection = Util.getConnection();
             PreparedStatement statement = connection.prepareStatement(sqlSelectAll)) {
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                User user = new User();
                user.setId(resultSet.getLong(1));
                user.setName(resultSet.getString(2));
                user.setLastName(resultSet.getString(3));
                user.setAge(resultSet.getByte(4));

                userList.add(user);
                logger.info("Table was printed");
            }
        } catch (SQLException e) {
            logger.warn("Table was NOT printed", e);
        }
        return userList;
    }

    public void cleanUsersTable() {

        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlCleanTable)) {
            connection.setAutoCommit(false);
            preparedStatement.executeUpdate();
            connection.commit();
            System.out.println("All users was deleted from database");
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                logger.warn("users was NOT deleted from db", e);
            }
        }
    }

    public void closeConnection() {
        try {
            connection.close();
            logger.info("Connection was closed");
        } catch (SQLException e) {
            logger.warn("Failed to close connection", e);
        }
    }
}