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
        objective6();
        System.out.println("----------------");
        objective7("d010", "PR");
        System.out.println("----------------");
        objective4("PR");
        System.out.println("----------------");
        objective8("PR");
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
            waitForMili(100);
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
            waitForMili(100);
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
            System.err.println("SQL error code: " + e.getErrorCode());
            waitForMili(100);
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
            waitForMili(100);
        }
    }

    private static void objective6() {
        try (Connection conn = ConnectionFactory.getConnection();
             Statement stmt = conn.createStatement()
        ) {
            ResultSet rs;
            String best = "WITH best AS " +
                    "(SELECT * " +
                    "FROM salaries " +
                    "WHERE to_date = '9999-01-01' " +
                    "ORDER BY salary DESC " +
                    "LIMIT 1) " +
                    "SELECT first_name, last_name " +
                    "FROM employees " +
                    "WHERE emp_no IN " +
                    "(SELECT emp_no " +
                    "FROM best)";
            String worst = "WITH worst AS " +
                    "(SELECT * " +
                    "FROM salaries " +
                    "WHERE to_date = '9999-01-01' " +
                    "ORDER BY salary ASC " +
                    "LIMIT 1) " +
                    "SELECT first_name, last_name " +
                    "FROM employees " +
                    "WHERE emp_no IN " +
                    "(SELECT emp_no " +
                    "FROM worst)";

            rs = stmt.executeQuery(best);
            rs.next();
            System.out.println("Best paid man: " + rs.getString("first_name") +
                    " " + rs.getString("last_name"));
            rs = stmt.executeQuery(worst);
            rs.next();
            System.out.println("Worst paid man: " + rs.getString("first_name") +
                    " " + rs.getString("last_name"));
            rs.close();
        } catch (SQLException e) {
            System.err.println("SQL error code: " + e.getErrorCode());
            waitForMili(100);
        }
    }

    private static void objective7(String deptNo, String deptName) {
        int empNo = 1000002;
        int empNoManager = empNo + 1;

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

            addDept.setString(1, deptNo);
            addDept.setString(2, deptName);
            addDept.executeUpdate();

            addEmployee.setString(6, "2018-12-15");
            // first
            addEmployee.setInt(1, empNo);
            addEmployee.setString(2, "1990-01-01");
            addEmployee.setString(3, "Arek");
            addEmployee.setString(4, "Sekuła");
            addEmployee.setString(5, "M");
            addEmployee.executeUpdate();
            // second
            addEmployee.setInt(1, empNoManager);
            addEmployee.setString(2, "1991-12-12");
            addEmployee.setString(3, "Andrzej");
            addEmployee.setString(4, "Dupa");
            addEmployee.setString(5, "M");
            addEmployee.executeUpdate();

            // zwykły
            addEmployeeTable.setInt(1, empNo);
            addEmployeeTable.setString(2, deptNo);
            addEmployeeTable.setString(3, "2018-12-15");
            addEmployeeTable.setString(4, "9999-01-01");
            addEmployeeTable.executeUpdate();
            // manager
            addEmployeeManager.setInt(1, empNoManager);
            addEmployeeManager.setString(2, deptNo);
            addEmployeeManager.setString(3, "2018-12-15");
            addEmployeeManager.setString(4, "9999-01-01");
            addEmployeeManager.executeUpdate();

            addSalaries.setString(3, "2018-12-15");
            addSalaries.setString(4, "9999-01-01");
            // normalny
            addSalaries.setInt(1, empNo);
            addSalaries.setInt(2, 2000);
            addSalaries.executeUpdate();
            // manager
            addSalaries.setInt(1, empNoManager);
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

    private static void objective8(String deptToDel) {
//        final String deleteEmployeesStmt = "DELETE FROM employees ee " +
//                "WHERE ee.emp_no IN (SELECT e.emp_no FROM " +
//                "employees e INNER JOIN dept_emp de " +
//                "ON e.emp_no = dm.emp_no INNER JOIN departments d " +
//                "ON d.dept_no = dm.dept_no WHERE dept_name = ?)";
        final String deleteSalaries = "WITH todelete AS\n" +
                "    (SELECT e.emp_no\n" +
                "    FROM employees e\n" +
                "\tINNER JOIN dept_manager dm ON e.emp_no = dm.emp_no\n" +
                "\tINNER JOIN departments d ON dm.dept_no = d.dept_no\n" +
                "    WHERE dept_name = ?\n" +
                "    UNION ALL\n" +
                "\tSELECT e.emp_no\n" +
                "    FROM employees e\n" +
                "    INNER JOIN dept_emp de ON e.emp_no = de.emp_no\n" +
                "\tINNER JOIN departments d ON de.dept_no = d.dept_no\n" +
                "    WHERE dept_name = ?)\n" +
                "DELETE FROM salaries\n" +
                "WHERE emp_no IN\n" +
                "\t(SELECT emp_no\n" +
                "    FROM todelete)";
        final String deleteEmployeesStmt = "WITH todelete AS\n" +
                "    (SELECT e.emp_no\n" +
                "    FROM employees e\n" +
                "\tINNER JOIN dept_manager dm ON e.emp_no = dm.emp_no\n" +
                "\tINNER JOIN departments d ON dm.dept_no = d.dept_no\n" +
                "    WHERE dept_name = ?\n" +
                "    UNION ALL\n" +
                "\tSELECT e.emp_no\n" +
                "    FROM employees e\n" +
                "    INNER JOIN dept_emp de ON e.emp_no = de.emp_no\n" +
                "\tINNER JOIN departments d ON de.dept_no = d.dept_no\n" +
                "    WHERE dept_name = ?)\n" +
                "DELETE FROM employees\n" +
                "WHERE emp_no IN\n" +
                "\t(SELECT emp_no\n" +
                "    FROM todelete)";
        final String deleteDeptEmp = "WITH todelete AS\n" +
                "\t(SELECT de.dept_no\n" +
                "\tFROM dept_emp de\n" +
                "    INNER JOIN departments d ON d.dept_no = de.dept_no\n" +
                "    WHERE dept_name = ?)\n" +
                "DELETE FROM dept_emp\n" +
                "WHERE dept_no IN\n" +
                "\t(SELECT dept_no\n" +
                "    FROM todelete)";
        final String deleteDeptManager = "WITH todelete AS\n" +
                "\t(SELECT dm.dept_no\n" +
                "\tFROM dept_manager dm\n" +
                "    INNER JOIN departments d ON d.dept_no = dm.dept_no\n" +
                "    WHERE dept_name = ?)\n" +
                "DELETE FROM dept_manager\n" +
                "WHERE dept_no IN\n" +
                "\t(SELECT dept_no\n" +
                "    FROM todelete)";
        final String deleteDepartment = "DELETE FROM departments " +
                "WHERE dept_name = ?";

        try (Connection conn = ConnectionFactory.getConnection();
             PreparedStatement psSal = conn.prepareStatement(deleteSalaries);
             PreparedStatement psEmp = conn.prepareStatement(deleteEmployeesStmt);
             PreparedStatement psDelDeptEmp = conn.prepareStatement(deleteDeptEmp);
             PreparedStatement psDelDeptManager = conn.prepareStatement(deleteDeptManager);
             PreparedStatement psDept = conn.prepareStatement(deleteDepartment)
        ) {
            conn.setAutoCommit(false);

            psSal.setString(1, deptToDel);
            psSal.setString(2, deptToDel);
            psSal.executeUpdate();
            psEmp.setString(1, deptToDel);
            psEmp.setString(2, deptToDel);
            psEmp.executeUpdate();
            psDelDeptEmp.setString(1, deptToDel);
            psDelDeptEmp.executeUpdate();
            psDelDeptManager.setString(1, deptToDel);
            psDelDeptManager.executeUpdate();
            psDept.setString(1, deptToDel);
            psDept.executeUpdate();

//            conn.rollback();
            conn.commit();
            System.out.println("Deleting " + deptToDel + " department done.");
        } catch (SQLException e) {
//            e.printStackTrace();
            System.err.println("SQL error code: " + e.getErrorCode());
            waitForMili(100);
        }
    }
}
