package employees.basic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

class ConnectionFactory {
    static Connection getConnection() throws SQLException {
            return DriverManager.getConnection(
//                "jdbc:mysql://localhost/employees?serverTimezone=UTC&useSSL=false",
                    "jdbc:mysql://localhost/employees" +
                            "?serverTimezone=CET" +
                            "&useSSL=false",
                    "arqarq",
                                                                                                                            "j000zwiem"
            );
    }
}
