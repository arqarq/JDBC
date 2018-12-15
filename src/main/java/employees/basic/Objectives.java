package employees.basic;

import java.sql.*;

public class Objectives {
    public static void main(String[] args) {
//        objective2("SELECT count(*) FROM departments", "SELECT count(*) FROM employees");
        System.out.println("----------------");
        objective3("Finance");
    }

    private static void objective2(String... query) {
        try (Connection conn = ConnectionFactory.getConnection();
             Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query[0]);
            if (rs.next()) {
                System.out.println("Ilość departamentów: " + rs.getInt(1));
            }
            rs = stmt.executeQuery(query[1]);
            if (rs.next()) {
                System.out.println("Ilość pracowników: " + rs.getInt(1));
            }
            rs.close();
        } catch (SQLException e) {
            System.out.println(e.getErrorCode());
        }
    }

    private static void objective3(String deptName) {
        try (Connection conn = ConnectionFactory.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM departments WHERE dept_name = ?")) {
            stmt.setString(1, deptName);
            stmt.executeUpdate();
            ResultSet rs = stmt.executeQuery();
            rs.next();
            System.out.println(rs.getString(1));
            rs.close();
        } catch (SQLException e) {
            System.out.println(e.getErrorCode());
        }
    }
}
