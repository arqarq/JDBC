package employees.basic;

import employees.common.ConnectionFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static employees.common.HelperMethods.waitForMili;

class Excercise9 {
    static void objective9(String deptName, int salaryToSet) {
        final String query = "WITH workers AS\n" +
                "\t(SELECT de.emp_no, de.dept_no\n" +
                "\tFROM salaries s, dept_emp de\n" +
                "\tWHERE s.emp_no = de.emp_no\n" +
                "\t\tUNION ALL\n" +
                "\tSELECT dm.emp_no, dm.dept_no\n" +
                "\tFROM salaries s, dept_manager dm\n" +
                "\tWHERE s.emp_no = dm.emp_no)\n" +
                "UPDATE salaries\n" +
                "SET salary = ?\n" +
                "WHERE emp_no IN\n" +
                "\t(SELECT w.emp_no\n" +
                "    FROM workers w\n" +
                "    INNER JOIN departments d ON d.dept_no = w.dept_no\n" +
                "    WHERE dept_name = ?)\n" +
                "    AND to_date = '9999-01-01'";

        try (Connection conn = ConnectionFactory.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)
        ) {
            conn.setAutoCommit(false);

            stmt.setInt(1, salaryToSet);
            stmt.setString(2, deptName);
            int i = stmt.executeUpdate();

//            conn.rollback();
            conn.commit();
            System.out.println("Task 9 commit done, " + i + " row(s) affected.");
        } catch (SQLException e) {
//            e.printStackTrace();
            System.err.println("SQL error code: " + e.getErrorCode());
            waitForMili(100);
        }
    }
}
