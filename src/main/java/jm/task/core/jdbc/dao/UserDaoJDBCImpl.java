package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;
import lombok.extern.slf4j.Slf4j;
import lombok.extern.slf4j.XSlf4j;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@XSlf4j
public class UserDaoJDBCImpl implements UserDao {
    private final Connection connection = Util.getConnection();
    private final String sqlCreateTable = "CREATE TABLE IF NOT EXISTS \"user\" " + "(id BIGSERIAL PRIMARY KEY, name VARCHAR(45), " + "lastName VARCHAR(45), age SMALLINT)";
    private final String sqlDropTable = "DROP TABLE IF EXISTS \"user\"";
    private final String sqlSaveUser = "INSERT INTO \"user\" (name, lastName,age)" + "VALUES (?,?,?)";
    private final String sqlDeleteById = "DELETE FROM \"user\" WHERE id = ?";
    private final String sqlSelectAll = "SELECT * FROM \"user\"";
    private final String sqlCleanTable = "TRUNCATE TABLE \"user\"";

    public void createUsersTable() {
        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlCreateTable)) {
            preparedStatement.executeUpdate();
            log.info("Table was created");
        } catch (SQLException e) {
            log.error("Table was NOT created", e.getCause());
        }
    }

    public void dropUsersTable() {
        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlDropTable)) {
            connection.setAutoCommit(false);
            preparedStatement.executeUpdate();
            connection.commit();
            log.info("Table was dropped");
        } catch (SQLException e) {
            log.error("Table was NOT dropped", e.getCause());
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
                log.error("User was NOT saved", e.getCause());
            }
        }
    }

    public void removeUserById(long id) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlDeleteById)) {
            connection.setAutoCommit(false);

            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();

            connection.commit();
            log.info("User with ID " + id + " was deleted ");
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                log.error("User with ID was NOT deleted", e.getCause());
            }
        }
    }

    public List<User> getAllUsers() {
        List<User> userList = new ArrayList<>();
        try (Connection connection = Util.getConnection(); PreparedStatement statement = connection.prepareStatement(sqlSelectAll)) {
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                User user = new User();
                user.setId(resultSet.getLong(1));
                user.setName(resultSet.getString(2));
                user.setLastName(resultSet.getString(3));
                user.setAge(resultSet.getByte(4));

                userList.add(user);
                log.info("Table was printed");
            }
        } catch (SQLException e) {
            log.error("Table was NOT printed", e.getCause());
        }
        return userList;
    }

    public void cleanUsersTable() {

        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlCleanTable);) {
            connection.setAutoCommit(false);
            preparedStatement.executeUpdate();
            connection.commit();
            System.out.println("All users was deleted from database");
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                log.error("users was NOT deleted from db", e.getCause());
            }
        }
    }

    public void closeConnection() {
        try {
            connection.close();
            log.info("Connection was closed");
        } catch (SQLException e) {
            log.error("Failed to close connection", e.getCause());
        }
    }
}