package at.htl.erikmayrhofer;


import org.junit.*;
import org.junit.runners.MethodSorters;

import javax.xml.transform.Result;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.time.temporal.TemporalAmount;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DBTest {

    public static final String DRIVER_STRING = "org.apache.derby.jdbc.ClientDriver";
    public static final String CONNECTION_STRING = "jdbc:derby://localhost:1527/db";
    public static final String USER = "app";
    public static final String PASSWORD= "app";
    public static Connection conn;

    @BeforeClass
    public static void initJDBC(){
        try{
            Class.forName(DRIVER_STRING);
            conn = DriverManager.getConnection(CONNECTION_STRING, USER, PASSWORD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Verbindung zur Datenbank nicht möglich\n"+e.getMessage()+"\n");
            e.printStackTrace();
        }

        //Create tables

        try{
            Statement stmt = conn.createStatement();
            String crtSubjectString = "CREATE TABLE subject (" +
                    "id INT CONSTRAINT subject_pk PRIMARY KEY " +
                    "GENERATED ALWAYS AS IDENTITY(START WITH 1, INCREMENT BY 1)," +
                    "name VARCHAR(255) NOT NULL UNIQUE" +
                    ")";
            stmt.execute(crtSubjectString);
            String crtAssignmentString = "CREATE TABLE assignment(" +
                    "id INT CONSTRAINT assignment_pk PRIMARY KEY " +
                    "GENERATED ALWAYS AS IDENTITY(START WITH 1, INCREMENT BY 1)," +
                    "duedate DATE," +
                    "description VARCHAR(2000)," +
                    "subject INT CONSTRAINT assignemnt_subject_fk REFERENCES subject(id)" +
                    ")";
            stmt.execute(crtAssignmentString);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void t01_dml(){
        int countInserts = 0;
        try{
            String insertSubString = "INSERT INTO subject(name) VALUES(?)";
            PreparedStatement subsmt = conn.prepareStatement(insertSubString);
            subsmt.setString(1, "Deutsch");
            countInserts += subsmt.executeUpdate();
            subsmt.setString(1, "Englisch");
            countInserts += subsmt.executeUpdate();
            subsmt.setString(1, "Mathematik");
            countInserts += subsmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        assertThat(countInserts, is(3));

        try {
            PreparedStatement psmt = conn.prepareStatement("SELECT name FROM subject");
            ResultSet rs = psmt.executeQuery();
            rs.next();
            assertThat(rs.getString(1), is("Deutsch"));
            rs.next();
            assertThat(rs.getString(1), is("Englisch"));
            rs.next();
            assertThat(rs.getString(1), is("Mathematik"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void t02_assignment(){
        int countInserts = 0;
        try{
            String insertAssString = "INSERT INTO assignment(duedate, description, subject)" +
                    "VALUES(?, ?, (SELECT id FROM subject WHERE name = ?))";
            PreparedStatement psmt = conn.prepareStatement(insertAssString);
            psmt.setDate(1, java.sql.Date.valueOf(LocalDate.now().plus(Period.ofDays(2))));
            psmt.setString(2, "Testassignment");
            psmt.setString(3, "Deutsch");
            countInserts += psmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        assertThat(countInserts, is(1));

        try{
            String getAssString = "SELECT a.duedate, a.description, s.name FROM assignment a " +
                    "LEFT JOIN subject s ON(a.subject = s.id)";
            PreparedStatement psmt = conn.prepareStatement(getAssString);
            ResultSet rs = psmt.executeQuery();
            rs.next();
            assertThat(rs.getString(2), is("Testassignment"));
            assertThat(rs.getString(3), is("Deutsch"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void teardownJDBC(){
        //Drop tables
        try{
            Statement stmt = conn.createStatement();
            String dropAssignmentString = "DROP TABLE assignment";
            stmt.execute(dropAssignmentString);
            String dropSubjectString = "DROP TABLE subject";
            stmt.execute(dropSubjectString);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
