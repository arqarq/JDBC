package employees.basic;

import employees.common.ConnectionFactory;

import java.sql.*;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

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
        objective5("Maliniak");
        System.out.println("----------------");
        objective7();
        System.out.println("----------------");
//        objective8();
    }

    private static void waitForMili(long delay) {
        try {
            TimeUnit.MILLISECONDS.sleep(delay);
        } catch (InterruptedException e1) {
            System.err.println(Arrays.toString(e1.getStackTrace()));
            waitForMili(100);
        }
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
                     "WHERE d.dept_no = ?")) { // SELECT e.first_name, e.last_name FROM employees e , departments d , dept_manager dm WHERE e.emp_no = dm.emp_no AND dm.dept_no = d.dept_no AND d.dept_name = ?
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

    private static void objective5(String last_name) {
        try (Connection conn = ConnectionFactory.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT emp_no, first_name FROM employees " +
                     "WHERE last_name = ?")) {
            ResultSet rs;

            stmt.setString(1, last_name);
            rs = stmt.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getString("emp_no") + " " + rs.getString("first_name"));
            }
            rs.close();
        } catch (SQLException e) {
            System.out.println(e.getErrorCode());
        }
    }

    private static synchronized void objective7() {
        try (Connection conn = ConnectionFactory.getConnection();
             PreparedStatement addDept = conn.prepareStatement("INSERT INTO " +
                     "departments " +
                     "VALUES (?, ?)");
             PreparedStatement addEmployeeTable = conn.prepareStatement("INSERT INTO " +
                     "dept_emp " +
                     "VALUES (?, ?, ?, ?)");
             PreparedStatement addEmployeeManager = conn.prepareStatement("INSERT INTO " +
                     "dept_manager " +
                     "VALUES (?, ?, ?, ?)");
             PreparedStatement addEmployee = conn.prepareStatement("INSERT INTO " +
                     "employees " +
                     "VALUES (?, ?, ?, ?, ?, ?)");
             PreparedStatement addSalaries = conn.prepareStatement("INSERT INTO " +
                     "salaries " +
                     "VALUES (?, ?, ?, ?)")
        ) {
            conn.setAutoCommit(false);

            addDept.setString(1, "d010");
            addDept.setString(2, "PR");
            addDept.executeUpdate();

            addEmployee.setString(6, "2018-12-15");
            // first
            addEmployee.setInt(1, 1000000);
            addEmployee.setString(2, "1990-01-01");
            addEmployee.setString(3, "Arek");
            addEmployee.setString(4, "Sekuła");
            addEmployee.setString(5, "M");
            addEmployee.executeUpdate();
            // second
            addEmployee.setInt(1, 1000001);
            addEmployee.setString(2, "1991-12-12");
            addEmployee.setString(3, "Andrzej");
            addEmployee.setString(4, "Dupa");
            addEmployee.setString(5, "M");
            addEmployee.executeUpdate();

            // zwykły
            addEmployeeTable.setInt(1, 1000000);
            addEmployeeTable.setString(2, "d010");
            addEmployeeTable.setString(3, "2018-12-15");
            addEmployeeTable.setString(4, "9999-01-01");
            addEmployeeTable.executeUpdate();
            // manager
            addEmployeeManager.setInt(1, 1000001);
            addEmployeeManager.setString(2, "d010");
            addEmployeeManager.setString(3, "2018-12-15");
            addEmployeeManager.setString(4, "9999-01-01");
            addEmployeeManager.executeUpdate();

            addSalaries.setString(3, "2018-12-15");
            addSalaries.setString(4, "9999-01-01");
            // normalny
            addSalaries.setInt(1, 1000000);
            addSalaries.setInt(2, 2000);
            addSalaries.executeUpdate();
            // manager
            addSalaries.setInt(1, 1000001);
            addSalaries.setInt(2, 6000);
            addSalaries.executeUpdate();

//            conn.rollback();
            conn.commit();
            System.out.println("Task 7 commit done.");
        } catch (SQLException e) {
//            e.printStackTrace();
            System.err.println("SQL error code: " + e.getErrorCode());
            waitForMili(100);
        }
    }

    private static void objective8() {
        final String deleteEmployeesStmt = "DELETE FROM employees ee " +
                "WHERE ee.emp_no IN (SELECT e.emp_no FROM " +
                "employees e INNER JOIN dept_emp de " +
                "ON e.emp_no = dm.emp_no INNER JOIN departments d " +
                "ON d.dept_no = dm.dept_no WHERE dept_name = ?)";
        final String deleteDepartment = "DELETE FROM departments " +
                "WHERE dept_name = ?";
        try (Connection conn = ConnectionFactory.getConnection();
             PreparedStatement psEmp = conn.prepareStatement(deleteEmployeesStmt);
             PreparedStatement psDept = conn.prepareStatement(deleteDepartment)
        ) {
            psEmp.setString(1, "PR");
            psEmp.executeUpdate();
            psDept.setString(1, "PR");
            psDept.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e.getErrorCode());
        }
    }
}
