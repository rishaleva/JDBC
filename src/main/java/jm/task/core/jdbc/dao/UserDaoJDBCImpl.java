package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {
    Connection connection = Util.getConnection();

    private final String sqlCreateTable = "CREATE TABLE IF NOT EXISTS user" +
            "(id BIGINT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(45), " +
            "lastName VARCHAR(45), age TINYINT)";
    private final String sqlDropTable = "DROP TABLE IF EXISTS user";
    private final String sqlSaveUser = "INSERT INTO user (name, lastName,age)" + "VALUES (?,?,?)";
    private final String sqlDeleteById = "DELETE FROM user WHERE id = ?";
    private final String sqlSelectAll = "SELECT * FROM user";
    private final String sqlCleanTable = "TRUNCATE TABLE user";

    @Override
    public void createUsersTable() {
        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlCreateTable)) {
            connection.setAutoCommit(false);
            preparedStatement.executeUpdate();

            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
@Override
    public void dropUsersTable() {
        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlDropTable)) {

            preparedStatement.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
@Override
    public void saveUser(String name, String lastName, byte age) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlSaveUser)) {
            connection.setAutoCommit(false);

            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setByte(3, age);
            preparedStatement.executeUpdate();

            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }
@Override
    public void removeUserById(long id) {

        try ( PreparedStatement preparedStatement = connection.prepareStatement(sqlDeleteById);) {
            connection.setAutoCommit(false);

            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();

            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }
@Override
    public List<User> getAllUsers() {
        List<User> userList = new ArrayList<>();
        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlSelectAll)) {
            ResultSet resultSet = preparedStatement.executeQuery(sqlSelectAll);

            while (resultSet.next()) {
                User user = new User();
                user.setId(resultSet.getLong(1));
                user.setName(resultSet.getString(2));
                user.setLastName(resultSet.getString(3));
                user.setAge(resultSet.getByte(4));

                userList.add(user);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return userList;
    }
@Override
    public void cleanUsersTable() {
        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlCleanTable)) {

            connection.setAutoCommit(false);
            preparedStatement.executeUpdate();

            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }
}