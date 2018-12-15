package employees.basic;

import employees.common.ConnectionFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DisplayDepartments {
    private static final String query = "SELECT dept_no, dept_name FROM departments ORDER BY dept_no ASC";

    public static void main(String[] args) {
        try (Connection conn = ConnectionFactory.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                System.out.println(rs.getString("dept_no") + " " + rs.getString("dept_name"));
            }
        } catch (SQLException e) {
            System.out.println(e.getErrorCode());
        }
    }
}
