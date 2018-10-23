package at.htl.erikmayrhofer;


import org.junit.*;
import org.junit.runners.MethodSorters;

import java.sql.*;
import java.time.Instant;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DBTest {

    public static final String DRIVER_STRING = "org.apache.derby.jdbc.ClientDriver";
    public static final String CONNECTION_STRING = "jdbc:derby://localhost:1527/db";
    public static final String USER = "app";
    public static final String PASSWORD = "app";
    public static Connection conn;

    @BeforeClass
    public static void initJDBC() {
        try {
            Class.forName(DRIVER_STRING);
            conn = DriverManager.getConnection(CONNECTION_STRING, USER, PASSWORD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            Assert.fail();
        } catch (SQLException e) {
            System.err.println("Verbindung zur Datenbank nicht möglich\n" + e.getMessage() + "\n");
            e.printStackTrace();
            Assert.fail();
        }

        //Create tables

        try {
            Statement stmt = conn.createStatement();
            String crtSubjectString = "CREATE TABLE subject (" +
                    "id INT CONSTRAINT subject_pk PRIMARY KEY " +
                    "GENERATED ALWAYS AS IDENTITY(START WITH 1, INCREMENT BY 1)," +
                    "name VARCHAR(255) NOT NULL CONSTRAINT subject_name_uniq UNIQUE" +
                    ")";
            stmt.execute(crtSubjectString);
            String crtCourseString = "CREATE TABLE course(" +
                    "id INT CONSTRAINT course_pk PRIMARY KEY " +
                    "GENERATED ALWAYS AS IDENTITY(START WITH 1, INCREMENT BY 1)," +
                    "time TIMESTAMP," +
                    "description VARCHAR(1000)," +
                    "subject INT CONSTRAINT course_subject_fk REFERENCES subject(id) NOT NULL" +
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
                    "course INT CONSTRAINT registration_course_fk REFERENCES course(id) NOT NULL," +
                    "student INT CONSTRAINT registration_student_fk REFERENCES student(id) NOT NULL" +
                    ")";
            stmt.execute(crtRegistrationtring);


        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void t01_subjectmeta() {
        try {
            Statement stmt = conn.createStatement();
            ResultSet set = stmt.executeQuery(
                    "SELECT columnname, CAST(COLUMNDATATYPE AS VARCHAR(200)) FROM " +
                            "sys.SYSCOLUMNS c LEFT JOIN sys.SYSTABLES t ON(c.REFERENCEID = t.TABLEID)" +
                            "WHERE UPPER(t.TABLENAME) = 'SUBJECT' ORDER BY c.COLUMNNAME"
            );
            set.next();
            assertThat(set.getString(1), is("ID"));
            assertThat(set.getString(2), is("INTEGER NOT NULL"));
            set.next();
            assertThat(set.getString(1), is("NAME"));
            assertThat(set.getString(2), is("VARCHAR(255) NOT NULL"));
            assertThat(set.next(), is(false));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT c.constraintname, type FROM SYS.SYSCONSTRAINTS c " +
                    "LEFT JOIN SYS.SYSTABLES t ON c.TABLEID = t.TABLEID " +
                    "WHERE t.TABLENAME = 'SUBJECT' ORDER BY c.CONSTRAINTNAME ASC");
            rs.next();
            assertThat(rs.getString(1), is("SUBJECT_NAME_UNIQ"));
            assertThat(rs.getString(2), is("U"));
            rs.next();
            assertThat(rs.getString(1), is("SUBJECT_PK"));
            assertThat(rs.getString(2), is("P"));
            assertThat(rs.next(), is(false));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void t02_coursemeta() {
        try {
            Statement stmt = conn.createStatement();
            ResultSet set = stmt.executeQuery(
                    "SELECT columnname, CAST(COLUMNDATATYPE AS VARCHAR(200)) FROM " +
                            "sys.SYSCOLUMNS c LEFT JOIN sys.SYSTABLES t ON(c.REFERENCEID = t.TABLEID)" +
                            "WHERE UPPER(t.TABLENAME) = 'COURSE' ORDER BY c.COLUMNNAME"
            );
            set.next();
            assertThat(set.getString(1), is("DESCRIPTION"));
            assertThat(set.getString(2), is("VARCHAR(1000)"));
            set.next();
            assertThat(set.getString(1), is("ID"));
            assertThat(set.getString(2), is("INTEGER NOT NULL"));
            set.next();
            assertThat(set.getString(1), is("SUBJECT"));
            assertThat(set.getString(2), is("INTEGER NOT NULL"));
            set.next();
            assertThat(set.getString(1), is("TIME"));
            assertThat(set.getString(2), is("TIMESTAMP"));
            assertThat(set.next(), is(false));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT c.constraintname, type FROM SYS.SYSCONSTRAINTS c " +
                    "LEFT JOIN SYS.SYSTABLES t ON c.TABLEID = t.TABLEID " +
                    "WHERE t.TABLENAME = 'COURSE' ORDER BY c.CONSTRAINTNAME ASC");
            rs.next();
            assertThat(rs.getString(1), is("COURSE_PK"));
            assertThat(rs.getString(2), is("P"));
            rs.next();
            assertThat(rs.getString(1), is("COURSE_SUBJECT_FK"));
            assertThat(rs.getString(2), is("F"));
            assertThat(rs.next(), is(false));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void t03_studentmeta() {
        try {
            Statement stmt = conn.createStatement();
            ResultSet set = stmt.executeQuery(
                    "SELECT columnname, CAST(COLUMNDATATYPE AS VARCHAR(200)) FROM " +
                            "sys.SYSCOLUMNS c LEFT JOIN sys.SYSTABLES t ON(c.REFERENCEID = t.TABLEID)" +
                            "WHERE UPPER(t.TABLENAME) = 'STUDENT' ORDER BY c.COLUMNNAME"
            );
            set.next();
            assertThat(set.getString(1), is("ID"));
            assertThat(set.getString(2), is("INTEGER NOT NULL"));
            set.next();
            assertThat(set.getString(1), is("NAME"));
            assertThat(set.getString(2), is("VARCHAR(30)"));
            assertThat(set.next(), is(false));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
            Assert.fail();
        }

        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT c.constraintname, type FROM SYS.SYSCONSTRAINTS c " +
                    "LEFT JOIN SYS.SYSTABLES t ON c.TABLEID = t.TABLEID " +
                    "WHERE t.TABLENAME = 'STUDENT' ORDER BY c.CONSTRAINTNAME ASC");
            rs.next();
            assertThat(rs.getString(1), is("STUDENT_PK"));
            assertThat(rs.getString(2), is("P"));
            assertThat(rs.next(), is(false));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void t04_registrationmeta() {
        try {
            Statement stmt = conn.createStatement();
            ResultSet set = stmt.executeQuery(
                    "SELECT columnname, CAST(COLUMNDATATYPE AS VARCHAR(200)) FROM " +
                            "sys.SYSCOLUMNS c LEFT JOIN sys.SYSTABLES t ON(c.REFERENCEID = t.TABLEID)" +
                            "WHERE UPPER(t.TABLENAME) = 'REGISTRATION' ORDER BY c.COLUMNNAME"
            );
            set.next();
            assertThat(set.getString(1), is("COURSE"));
            assertThat(set.getString(2), is("INTEGER NOT NULL"));
            set.next();
            assertThat(set.getString(1), is("ID"));
            assertThat(set.getString(2), is("INTEGER NOT NULL"));
            set.next();
            assertThat(set.getString(1), is("STUDENT"));
            assertThat(set.getString(2), is("INTEGER NOT NULL"));
            assertThat(set.next(), is(false));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT c.constraintname, type FROM SYS.SYSCONSTRAINTS c " +
                    "LEFT JOIN SYS.SYSTABLES t ON c.TABLEID = t.TABLEID " +
                    "WHERE t.TABLENAME = 'REGISTRATION' ORDER BY c.CONSTRAINTNAME ASC");
            rs.next();
            assertThat(rs.getString(1), is("REGISTRATION_COURSE_FK"));
            assertThat(rs.getString(2), is("F"));
            rs.next();
            assertThat(rs.getString(1), is("REGISTRATION_PK"));
            assertThat(rs.getString(2), is("P"));
            rs.next();
            assertThat(rs.getString(1), is("REGISTRATION_STUDENT_FK"));
            assertThat(rs.getString(2), is("F"));
            assertThat(rs.next(), is(false));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void t05_student() {
        int countInserts = 0;
        try {
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
            Assert.fail();
        }
        assertThat(countInserts, is(3));

        try {
            PreparedStatement psmt = conn.prepareStatement("SELECT name FROM student");
            ResultSet rs = psmt.executeQuery();
            rs.next();
            assertThat(rs.getString(1), is("Erik Mayrhofer"));
            rs.next();
            assertThat(rs.getString(1), is("Jan Neuburger"));
            rs.next();
            assertThat(rs.getString(1), is("Florian Schwarcz"));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void t06_subject() {
        int countInserts = 0;
        try {
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
            Assert.fail();
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
            Assert.fail();
        }
    }

    @Test
    public void t07_course() {
        int countInserts = 0;
        try {
            String insertSubString = "INSERT INTO course(time, description, subject) " +
                    "VALUES(?, ?, (SELECT id FROM SUBJECT WHERE NAME = ?))";
            PreparedStatement subsmt = conn.prepareStatement(insertSubString);
            subsmt.setTimestamp(1, Timestamp.from(Instant.now()));
            subsmt.setString(2, "Simple Algebra");
            subsmt.setString(3, "Mathematik");
            countInserts += subsmt.executeUpdate();
            subsmt.setTimestamp(1, Timestamp.from(Instant.now()));
            subsmt.setString(2, "Passive Voice");
            subsmt.setString(3, "Englisch");
            countInserts += subsmt.executeUpdate();
            subsmt.setTimestamp(1, Timestamp.from(Instant.now()));
            subsmt.setString(2, "Konjugation");
            subsmt.setString(3, "Deutsch");
            countInserts += subsmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
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
            Assert.fail();
        }
    }

    @Test
    public void t08_registration() {
        int countInserts = 0;
        try {
            String insertSubString = "INSERT INTO REGISTRATION(course, student) " +
                    "VALUES(" +
                    "(SELECT id FROM course WHERE DESCRIPTION = ?)," +
                    "(SELECT id FROM student WHERE NAME = ?))";
            PreparedStatement subsmt = conn.prepareStatement(insertSubString);
            subsmt.setString(1, "Simple Algebra");
            subsmt.setString(2, "Erik Mayrhofer");
            countInserts += subsmt.executeUpdate();
            subsmt.setString(1, "Konjugation");
            subsmt.setString(2, "Florian Schwarcz");
            countInserts += subsmt.executeUpdate();
            subsmt.setString(1, "Passive Voice");
            subsmt.setString(2, "Jan Neuburger");
            countInserts += subsmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
        assertThat(countInserts, is(3));

        try {
            PreparedStatement psmt = conn.prepareStatement(
                    "SELECT s.name student, c.DESCRIPTION course FROM " +
                            "registration r LEFT JOIN COURSE c on r.COURSE = c.ID LEFT JOIN " +
                            "student s ON r.STUDENT = s.ID"
            );
            ResultSet rs = psmt.executeQuery();
            rs.next();
            assertThat(rs.getString(1), is("Erik Mayrhofer"));
            assertThat(rs.getString(2), is("Simple Algebra"));
            rs.next();
            assertThat(rs.getString(1), is("Florian Schwarcz"));
            assertThat(rs.getString(2), is("Konjugation"));
            rs.next();
            assertThat(rs.getString(1), is("Jan Neuburger"));
            assertThat(rs.getString(2), is("Passive Voice"));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @AfterClass
    public static void teardownJDBC() {
        //Drop tables
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("DROP TABLE registration");
            stmt.execute("DROP TABLE course");
            stmt.execute("DROP TABLE subject");
            stmt.execute("DROP TABLE student");
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

}
