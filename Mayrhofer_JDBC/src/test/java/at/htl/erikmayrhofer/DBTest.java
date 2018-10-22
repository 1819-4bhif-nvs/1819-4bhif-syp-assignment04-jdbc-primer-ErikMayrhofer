package at.htl.erikmayrhofer;


import org.junit.*;
import org.junit.runners.MethodSorters;

import javax.xml.transform.Result;
import java.sql.*;
import java.time.*;
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
            String crtCourseString = "CREATE TABLE course(" +
                    "id INT CONSTRAINT course_pk PRIMARY KEY " +
                    "GENERATED ALWAYS AS IDENTITY(START WITH 1, INCREMENT BY 1)," +
                    "time TIMESTAMP," +
                    "description VARCHAR(1000)," +
                    "subject INT CONSTRAINT assignemnt_subject_fk REFERENCES subject(id)" +
                    ")";
            stmt.execute(crtCourseString);
            String crtStudentString = "CREATE TABLE student(" +
                    "id INT CONSTRAINT student_pk PRIMARY KEY " +
                    "GENERATED ALWAYS AS IDENTITY(START WITH 1, INCREMENT BY 1)," +
                    "name VARCHAR(30)" +
                    ")";
            stmt.execute(crtStudentString);
            String crtRegistrationtring = "CREATE TABLE registration(" +
                    "id INT CONSTRAINT registration_pk PRIMARY KEY " +
                    "GENERATED ALWAYS AS IDENTITY(START WITH 1, INCREMENT BY 1)," +
                    "course INT CONSTRAINT registration_course_fk REFERENCES course(id)," +
                    "student INT CONSTRAINT registration_student_fk REFERENCES student(id)" +
                    ")";
            stmt.execute(crtRegistrationtring);


        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void t01_subjectmeta() {
        try{
            Statement stmt = conn.createStatement();
            ResultSet set = stmt.executeQuery(
                    "SELECT columnname FROM " +
                    "sys.SYSCOLUMNS c LEFT JOIN sys.SYSTABLES t ON(c.REFERENCEID = t.TABLEID)" +
                            "WHERE UPPER(t.TABLENAME) = 'SUBJECT' ORDER BY c.COLUMNNAME"
            );
            set.next();
            assertThat(set.getString(1), is("ID"));
            set.next();
            assertThat(set.getString(1), is("NAME"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void t02_coursemeta() {
        try{
            Statement stmt = conn.createStatement();
            ResultSet set = stmt.executeQuery(
                    "SELECT columnname FROM " +
                            "sys.SYSCOLUMNS c LEFT JOIN sys.SYSTABLES t ON(c.REFERENCEID = t.TABLEID)" +
                            "WHERE UPPER(t.TABLENAME) = 'COURSE' ORDER BY c.COLUMNNAME"
            );
            set.next();
            assertThat(set.getString(1), is("DESCRIPTION"));
            set.next();
            assertThat(set.getString(1), is("ID"));
            set.next();
            assertThat(set.getString(1), is("SUBJECT"));
            set.next();
            assertThat(set.getString(1), is("TIME"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void t03_studentmeta() {
        try{
            Statement stmt = conn.createStatement();
            ResultSet set = stmt.executeQuery(
                    "SELECT columnname FROM " +
                            "sys.SYSCOLUMNS c LEFT JOIN sys.SYSTABLES t ON(c.REFERENCEID = t.TABLEID)" +
                            "WHERE UPPER(t.TABLENAME) = 'STUDENT' ORDER BY c.COLUMNNAME"
            );
            set.next();
            assertThat(set.getString(1), is("ID"));
            set.next();
            assertThat(set.getString(1), is("NAME"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void t04_registrationmeta() {
        try{
            Statement stmt = conn.createStatement();
            ResultSet set = stmt.executeQuery(
                    "SELECT columnname FROM " +
                            "sys.SYSCOLUMNS c LEFT JOIN sys.SYSTABLES t ON(c.REFERENCEID = t.TABLEID)" +
                            "WHERE UPPER(t.TABLENAME) = 'REGISTRATION' ORDER BY c.COLUMNNAME"
            );
            set.next();
            assertThat(set.getString(1), is("COURSE"));
            set.next();
            assertThat(set.getString(1), is("ID"));
            set.next();
            assertThat(set.getString(1), is("STUDENT"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void t05_student(){
        int countInserts = 0;
        try{
            String insertString = "INSERT INTO student(name) VALUES(?)";
            PreparedStatement subsmt = conn.prepareStatement(insertString);
            subsmt.setString(1, "Erik Mayrhofer");
            countInserts += subsmt.executeUpdate();
            subsmt.setString(1, "Jan Neuburger");
            countInserts += subsmt.executeUpdate();
            subsmt.setString(1, "Florian Schwarcz");
            countInserts += subsmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        assertThat(countInserts, is(3));

        try {
            PreparedStatement psmt = conn.prepareStatement("SELECT name FROM subject");
            ResultSet rs = psmt.executeQuery();
            rs.next();
            assertThat(rs.getString(1), is("Erik Mayrhofer"));
            rs.next();
            assertThat(rs.getString(1), is("Jan Neuburger"));
            rs.next();
            assertThat(rs.getString(1), is("Florian Schwarcz"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void t06_subject(){
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
    public void t07_course(){
        int countInserts = 0;
        try{
            String insertSubString = "INSERT INTO course(time, description, subject) " +
                    "VALUES(?, ?, (SELECT id FROM SUBJECT WHERE NAME = ?))";
            PreparedStatement subsmt = conn.prepareStatement(insertSubString);
            subsmt.setTimestamp(1, Timestamp.from(Instant.now()));
            subsmt.setString(2,"Simple Algebra");
            subsmt.setString(3, "Mathematik");
            countInserts += subsmt.executeUpdate();
            subsmt.setTimestamp(1, Timestamp.from(Instant.now()));
            subsmt.setString(2,"Passive Voice");
            subsmt.setString(3, "Englisch");
            countInserts += subsmt.executeUpdate();
            subsmt.setTimestamp(1, Timestamp.from(Instant.now()));
            subsmt.setString(2,"Konjugation");
            subsmt.setString(3, "Deutsch");
            countInserts += subsmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        assertThat(countInserts, is(3));

        try {
            PreparedStatement psmt = conn.prepareStatement(
                    "SELECT c.time, c.description, s.name FROM course c " +
                            "LEFT JOIN SUBJECT s ON (c.subject = s.id)"
            );
            ResultSet rs = psmt.executeQuery();
            rs.next();
            assertThat(rs.getString(2), is("Simple Algebra"));
            assertThat(rs.getString(3), is("Mathematik"));
            rs.next();
            assertThat(rs.getString(2), is("Passive Voice"));
            assertThat(rs.getString(3), is("Englisch"));
            rs.next();
            assertThat(rs.getString(2), is("Konjugation"));
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
            stmt.execute("DROP TABLE registration");
            stmt.execute("DROP TABLE course");
            stmt.execute("DROP TABLE subject");
            stmt.execute("DROP TABLE student");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
