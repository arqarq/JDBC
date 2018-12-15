package employees.basic;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class BasicSelect {
    private static final String query = "SELECT first_name, last_name FROM employees LIMIT 10";

    public static void main(String[] args) throws SQLException {
        Connection conn = ConnectionFactory.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {
            System.out.println(rs.getString("first_name") + " " + rs.getString("last_name"));
        }
        rs.close();
        stmt.close();
        conn.close();
    }
}
