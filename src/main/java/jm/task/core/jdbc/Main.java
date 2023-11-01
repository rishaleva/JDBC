package jm.task.core.jdbc;

import jm.task.core.jdbc.service.UserService;
import jm.task.core.jdbc.service.UserServiceImpl;

public class Main {
    public static void main(String[] args) {
        UserService userService = new UserServiceImpl();

        userService.createUsersTable();
        userService.saveUser("Name1", "LastName1", (byte) 22);
        userService.saveUser("Name2", "LastName2", (byte) 51);
        userService.saveUser("Name3", "LastName3", (byte) 22);
        userService.saveUser("Name4", "LastName4", (byte) 60);

        userService.getAllUsers();
        userService.cleanUsersTable();
        userService.dropUsersTable();
    }
}
