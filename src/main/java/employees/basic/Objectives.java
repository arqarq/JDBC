package employees.basic;

import java.sql.*;

public class Objectives {
    public static void main(String[] args) {
        objective2("SELECT count(*) FROM departments", "SELECT count(*) FROM employees");
        System.out.println("----------------");
        objective3("Finance");
        System.out.println("----------------");
        objective3("Marketing");
        System.out.println("----------------");
        objective3("Customer Service");
        System.out.println("----------------");
        objective4("Customer Service");
        System.out.println("----------------");

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
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM departments WHERE dept_name = ?");
             PreparedStatement stmt2 = conn.prepareStatement("SELECT first_name, last_name FROM employees e " +
                     "INNER JOIN dept_manager dm ON e.emp_no = dm.emp_no " +
                     "INNER JOIN departments d ON dm.dept_no = d.dept_no " +
                     "WHERE d.dept_no = ?")) {
            ResultSet rs;

            stmt.setString(1, deptName);
            rs = stmt.executeQuery();
            rs.next();
            String deptNo = rs.getString(1);

            System.out.println(deptName + ":");

            stmt2.setString(1, deptNo);
            rs = stmt2.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getString(1) + " " + rs.getString(2));
            }
            rs.close();
        } catch (SQLException e) {
            System.out.println(e.getErrorCode());
        }
    }

    private static void objective4(String deptName) {
        try (Connection conn = ConnectionFactory.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM departments WHERE dept_name = ?");
             PreparedStatement stmt2 = conn.prepareStatement("SELECT first_name, last_name FROM employees e " +
                     "INNER JOIN dept_manager dm ON e.emp_no = dm.emp_no " +
                     "INNER JOIN departments d ON dm.dept_no = d.dept_no " +
                     "WHERE d.dept_no = ? AND to_date = '9999-01-01'")) {
            ResultSet rs;

            stmt.setString(1, deptName);
            rs = stmt.executeQuery();
            rs.next();
            String deptNo = rs.getString(1);

            System.out.print(deptName + " - current manager: ");

            stmt2.setString(1, deptNo);
            rs = stmt2.executeQuery();
            rs.next();
            System.out.println(rs.getString(1) + " " + rs.getString(2));
            rs.close();
        } catch (SQLException e) {
            System.out.println(e.getErrorCode());
        }
    }
}
